/*
 * Copyright 2015 Groupon.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.tsdcore.sinks;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.http.javadsl.model.StatusCodes;
import akka.pattern.PatternsCS;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.RequestEntry;
import com.google.common.base.Charsets;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Actor that sends HTTP requests via a Ning HTTP client.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HttpPostSinkActor extends AbstractActor {
    /**
     * Factory method to create a Props.
     *
     * @param client Http client to create requests from.
     * @param sink Sink that controls request creation and data serialization.
     * @param maximumConcurrency Maximum number of concurrent requests.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param spreadPeriod Maximum time to delay sending new aggregates to spread load.
     * @param metricsFactory Metrics Factory to record metrics for the actor.
     * @return A new Props
     */
    public static Props props(
            final AsyncHttpClient client,
            final HttpPostSink sink,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadPeriod,
            final MetricsFactory metricsFactory) {
        return Props.create(HttpPostSinkActor.class, client, sink, maximumConcurrency, maximumQueueSize, spreadPeriod, metricsFactory);
    }

    /**
     * Public constructor.
     *
     * @param client Http client to create requests from.
     * @param sink Sink that controls request creation and data serialization.
     * @param maximumConcurrency Maximum number of concurrent requests.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param spreadPeriod Maximum time to delay sending new aggregates to spread load.
     * @param metricsFactory Metrics Factory to record metrics for the actor.
     */
    public HttpPostSinkActor(
            final AsyncHttpClient client,
            final HttpPostSink sink,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadPeriod,
            final MetricsFactory metricsFactory) {
        _client = client;
        _sink = sink;
        _maximumConcurrency = maximumConcurrency;
        _pendingRequests = EvictingQueue.create(maximumQueueSize);
        if (Objects.equals(Duration.ZERO, spreadPeriod)) {
            _spreadingDelayMillis = 0;
        } else {
            _spreadingDelayMillis = new Random().nextInt((int) spreadPeriod.toMillis());
        }
        _metricsFactory = metricsFactory;
        _evictedRequestsName = "sinks/http_post/" + sink.getMetricSafeName() + "/evicted_requests";
        _requestLatencyName = "sinks/http_post/" + sink.getMetricSafeName() + "/request_latency";
        _inQueueLatencyName = "sinks/http_post/" + sink.getMetricSafeName() + "/queue_time";
        _requestSuccessName = "sinks/http_post/" + sink.getMetricSafeName() + "/success";
        _responseStatusName = "sinks/http_post/" + sink.getMetricSafeName() + "/status";
        _samplesDroppedName = "sinks/http_post/" + sink.getMetricSafeName() + "/samples_dropped";
        _samplesSentName = "sinks/http_post/" + sink.getMetricSafeName() + "/samples_sent";
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("sink", _sink)
                .put("maximumConcurrency", _maximumConcurrency)
                .put("spreadingDelayMillis", _spreadingDelayMillis)
                .put("waiting", _waiting)
                .put("inflightRequestsCount", _inflightRequestsCount)
                .put("pendingRequestsCount", _pendingRequests.size())
                .put("metricsFactory", _metricsFactory)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(EmitAggregation.class, this::processEmitAggregation)
                .match(PostSuccess.class, success -> {
                   processSuccessRequest(success);
                    dispatchPending();
                })
                .match(PostRejected.class, rejected -> {
                    processRejectedRequest(rejected);
                    dispatchPending();
                })
                .match(PostFailure.class, failure -> {
                    processFailedRequest(failure);
                    dispatchPending();
                })
                .match(WaitTimeExpired.class, message -> {
                    LOGGER.debug()
                            .setMessage("Received WaitTimeExpired message")
                            .addContext("actor", self())
                            .log();
                    _waiting = false;
                    dispatchPending();
                })
                .build();
    }

    private void processFailedRequest(final PostFailure failure) {
        _inflightRequestsCount--;
        POST_ERROR_LOGGER.error()
                .setMessage("Post error")
                .addData("sink", _sink)
                .addContext("actor", self())
                .setThrowable(failure.getCause())
                .log();
    }

    private void processSuccessRequest(final PostSuccess success) {
        _postRequests++;
        _inflightRequestsCount--;
        final Response response = success.getResponse();
        LOGGER.debug()
                .setMessage("Post accepted")
                .addData("sink", _sink)
                .addData("status", response.getStatusCode())
                .addContext("actor", self())
                .log();
    }

    private void processRejectedRequest(final PostRejected rejected) {
        _postRequests++;
        _inflightRequestsCount--;
        final Response response = rejected.getResponse();
        final Optional<String> responseBody = Optional.ofNullable(response.getResponseBody());
        LOGGER.warn()
                .setMessage("Post rejected")
                .addData("sink", _sink)
                .addData("status", response.getStatusCode())
                // CHECKSTYLE.OFF: IllegalInstantiation - This is ok for String from byte[]
                .addData("request", new String(rejected.getRequest().getByteData(), Charsets.UTF_8))
                // CHECKSTYLE.ON: IllegalInstantiation
                .addData("response", responseBody)
                .addContext("actor", self())
                .log();
    }

    private void processEmitAggregation(final EmitAggregation emitMessage) {
        final PeriodicData periodicData = emitMessage.getData();

        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", _sink)
                .addData("dataSize", periodicData.getData().size())
                .addContext("actor", self())
                .log();

        if (!periodicData.getData().isEmpty()) {
            final Collection<RequestEntry.Builder> requestEntryBuilders = _sink.createRequests(_client, periodicData);
            final boolean pendingWasEmpty = _pendingRequests.isEmpty();

            final int evicted = Math.max(0, requestEntryBuilders.size() - _pendingRequests.remainingCapacity());
            for (final RequestEntry.Builder requestEntryBuilder : requestEntryBuilders) {
                // TODO(vkoskela): Add logging to client [MAI-89]
                // TODO(vkoskela): Add instrumentation to client [MAI-90]
                _pendingRequests.offer(requestEntryBuilder.setEnterTime(System.currentTimeMillis()).build());
            }

            if (evicted > 0) {
                // TODO(qinyanl): Convert to periodic metric in the future.
                try (Metrics metrics = _metricsFactory.create()) {
                    metrics.incrementCounter(_evictedRequestsName, evicted);
                }
                EVICTED_LOGGER.warn()
                        .setMessage("Evicted data from HTTP sink queue")
                        .addData("sink", _sink)
                        .addData("count", evicted)
                        .addContext("actor", self())
                        .log();
            }

            // If we don't currently have anything in-flight, we'll need to wait the spreading duration.
            // If we're already waiting, these requests will be sent after the waiting is over, no need to do anything else.
            if (pendingWasEmpty && !_waiting && _spreadingDelayMillis > 0) {
                _waiting = true;
                LOGGER.debug()
                        .setMessage("Scheduling http requests for later transmission")
                        .addData("delayMs", _spreadingDelayMillis)
                        .addContext("actor", self())
                        .log();
                context().system().scheduler().scheduleOnce(
                        FiniteDuration.apply(_spreadingDelayMillis, TimeUnit.MILLISECONDS),
                        self(),
                        new WaitTimeExpired(),
                        context().dispatcher(),
                        self());
            } else {
                dispatchPending();
            }
        }
    }

    /**
     * Dispatches the number of pending requests needed to drain the pendingRequests queue or meet the maximum concurrency.
     */
    private void dispatchPending() {
        LOGGER.debug()
                .setMessage("Dispatching requests")
                .addContext("actor", self())
                .log();
        while (_inflightRequestsCount < _maximumConcurrency && !_pendingRequests.isEmpty()) {
            fireNextRequest();
        }
    }

    private void fireNextRequest() {
        final RequestEntry requestEntry = _pendingRequests.poll();
        final Metrics metrics = _metricsFactory.create();
        metrics.setTimer(_inQueueLatencyName, System.currentTimeMillis() - requestEntry.getEnterTime(), TimeUnit.MILLISECONDS);

        final Request request = requestEntry.getRequest();
        _inflightRequestsCount++;

        final CompletableFuture<Response> promise = new CompletableFuture<>();
        final long requestStartTime = System.currentTimeMillis();
        _client.executeRequest(request, new ResponseAsyncCompletionHandler(promise));
        final CompletionStage<Object> responsePromise = promise
                .handle((result, err) -> {
                    metrics.setTimer(_requestLatencyName, System.currentTimeMillis() - requestStartTime, TimeUnit.MILLISECONDS);
                    final Object returnValue;
                    if (err == null) {
                        final int responseStatusCode = result.getStatusCode();
                        final int responseStatusClass = responseStatusCode / 100;
                        for (final int i : STATUS_CLASSES) {
                            metrics.incrementCounter(
                                    String.format("%s/%dxx", _responseStatusName, i),
                                    responseStatusClass == i ? 1 : 0);
                        }
                        if (ACCEPTED_STATUS_CODES.contains(responseStatusCode)) {
                             returnValue =  new PostSuccess(result);
                             metrics.incrementCounter(_samplesSentName, requestEntry.getPopulationSize());
                        } else {
                             returnValue =  new PostRejected(request, result);
                             metrics.incrementCounter(_samplesDroppedName, requestEntry.getPopulationSize());
                        }
                    } else {
                        returnValue = new PostFailure(request, err);
                        metrics.incrementCounter(_samplesDroppedName, requestEntry.getPopulationSize());
                    }
                    metrics.incrementCounter(_requestSuccessName, (returnValue instanceof PostSuccess) ? 1 : 0);
                    metrics.close();
                    return returnValue;
                });
        PatternsCS.pipe(responsePromise, context().dispatcher()).to(self());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        LOGGER.info()
                .setMessage("Shutdown sink actor")
                .addData("sink", _sink)
                .addData("recordsWritten", _postRequests)
                .log();
    }

    private int _inflightRequestsCount = 0;
    private long _postRequests = 0;
    private boolean _waiting = false;
    private final int _maximumConcurrency;
    private final EvictingQueue<RequestEntry> _pendingRequests;
    private final AsyncHttpClient _client;
    private final HttpPostSink _sink;
    private final int _spreadingDelayMillis;
    private final MetricsFactory _metricsFactory;

    private final String _evictedRequestsName;
    private final String _requestLatencyName;
    private final String _inQueueLatencyName;
    private final String _requestSuccessName;
    private final String _responseStatusName;
    private final String _samplesDroppedName;
    private final String _samplesSentName;


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpPostSink.class);
    private static final Logger POST_ERROR_LOGGER = LoggerFactory.getRateLimitLogger(HttpPostSink.class, Duration.ofSeconds(30));
    private static final Logger EVICTED_LOGGER = LoggerFactory.getRateLimitLogger(HttpPostSink.class, Duration.ofSeconds(30));
    private static final Set<Integer> ACCEPTED_STATUS_CODES = Sets.newHashSet();
    private static final com.google.common.collect.ImmutableList<Integer> STATUS_CLASSES = ImmutableList.of(2, 3, 4, 5);


    static {
        // TODO(vkoskela): Make accepted status codes configurable [AINT-682]
        ACCEPTED_STATUS_CODES.add(StatusCodes.OK.intValue());
        ACCEPTED_STATUS_CODES.add(StatusCodes.CREATED.intValue());
        ACCEPTED_STATUS_CODES.add(StatusCodes.ACCEPTED.intValue());
        ACCEPTED_STATUS_CODES.add(StatusCodes.NO_CONTENT.intValue());
    }

    /**
     * Message class to wrap a list of {@link AggregatedData}.
     */
    public static final class EmitAggregation {

        /**
         * Public constructor.
         *
         * @param data Periodic data to emit.
         */
        EmitAggregation(final PeriodicData data) {
            _data = data;
        }

        public PeriodicData getData() {
            return _data;
        }

        private final PeriodicData _data;
    }

    /**
     * Message class to wrap an errored HTTP request.
     */
    private static final class PostFailure {
        private PostFailure(final Request request, final Throwable throwable) {
            _request = request;
            _throwable = throwable;
        }

        public Request getRequest() {
            return _request;
        }

        public Throwable getCause() {
            return _throwable;
        }

        private final Request _request;
        private final Throwable _throwable;
    }

    /**
     * Message class to wrap a rejected HTTP request.
     */
    private static final class PostRejected {
        private PostRejected(final Request request, final Response response) {
            _request = request;
            _response = response;
        }

        public Request getRequest() {
            return _request;
        }

        public Response getResponse() {
            return _response;
        }

        private final Request _request;
        private final Response _response;
    }

    /**
     * Message class to wrap a successful HTTP request.
     */
    private static final class PostSuccess {
        private PostSuccess(final Response response) {
            _response = response;
        }

        public Response getResponse() {
            return _response;
        }

        private final Response _response;
    }

    /**
     * Message class to indicate that we are now able to send data.
     */
    private static final class WaitTimeExpired { }

    private static final class ResponseAsyncCompletionHandler extends AsyncCompletionHandler<Response> {

        ResponseAsyncCompletionHandler(final CompletableFuture<Response> promise) {
            _promise = promise;
        }

        @Override
        public Response onCompleted(final Response response) {
            _promise.complete(response);
            return response;
        }

        @Override
        public void onThrowable(final Throwable throwable) {
            _promise.completeExceptionally(throwable);
        }

        private final CompletableFuture<Response> _promise;
    }
}
