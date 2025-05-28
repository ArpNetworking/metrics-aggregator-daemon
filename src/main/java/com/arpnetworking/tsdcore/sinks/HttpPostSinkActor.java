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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.RequestEntry;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.Props;
import org.apache.pekko.pattern.Patterns;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import scala.concurrent.duration.FiniteDuration;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Actor that sends HTTP requests via a Ning HTTP client.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HttpPostSinkActor extends AbstractActorWithTimers {
    /**
     * Factory method to create a Props.
     *
     * @param client Http client to create requests from.
     * @param sink Sink that controls request creation and data serialization.
     * @param maximumConcurrency Maximum number of concurrent requests.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param spreadPeriod Maximum time to delay sending new aggregates to spread load.
     * @param periodicMetrics Periodic Metrics to record metrics for the actor.
     * @return A new Props
     */
    public static Props props(
            final AsyncHttpClient client,
            final HttpPostSink sink,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadPeriod,
            final PeriodicMetrics periodicMetrics) {
        return Props.create(HttpPostSinkActor.class, client, sink, maximumConcurrency, maximumQueueSize, spreadPeriod, periodicMetrics);
    }

    /**
     * Public constructor.
     *
     * @param client Http client to create requests from.
     * @param sink Sink that controls request creation and data serialization.
     * @param maximumConcurrency Maximum number of concurrent requests.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param spreadPeriod Maximum time to delay sending new aggregates to spread load.
     * @param periodicMetrics Periodic Metrics to record metrics for the actor.
     */
    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "Random is used to spread load, only used once is ok")
    public HttpPostSinkActor(
            final AsyncHttpClient client,
            final HttpPostSink sink,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadPeriod,
            final PeriodicMetrics periodicMetrics) {
        _client = client;
        _sink = sink;
        _acceptedStatusCodes = sink.getAcceptedStatusCodes();
        _maximumConcurrency = maximumConcurrency;
        _pendingRequests = EvictingQueue.create(maximumQueueSize);
        if (Objects.equals(Duration.ZERO, spreadPeriod)) {
            _spreadingDelayMillis = 0;
        } else {
            _spreadingDelayMillis = new Random().nextInt((int) spreadPeriod.toMillis());
        }
        _periodicMetrics = periodicMetrics;
        _evictedRequestsName = "sinks/http_post/" + sink.getMetricSafeName() + "/evicted_requests";
        _requestLatencyName = "sinks/http_post/" + sink.getMetricSafeName() + "/request_latency";
        _inQueueLatencyName = "sinks/http_post/" + sink.getMetricSafeName() + "/queue_time";
        _pendingRequestsQueueSizeName = "sinks/http_post/" + sink.getMetricSafeName() + "/queue_size";
        _inflightRequestsCountName = "sinks/http_post/" + sink.getMetricSafeName() + "/inflight_count";
        _requestSuccessName = "sinks/http_post/" + sink.getMetricSafeName() + "/success";
        _responseStatusName = "sinks/http_post/" + sink.getMetricSafeName() + "/status";
        _samplesDroppedName = "sinks/http_post/" + sink.getMetricSafeName() + "/samples_dropped";
        _samplesSentName = "sinks/http_post/" + sink.getMetricSafeName() + "/samples_sent";
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        timers().startTimerAtFixedRate("metrics", SampleMetrics.INSTANCE, Duration.ofSeconds(1), Duration.ofSeconds(1));

        LOGGER.info()
                .setMessage("Starting http post sink actor")
                .addData("actor", this)
                .addData("actorRef", self())
                .log();

        _periodicMetrics.recordCounter("actors/http_post_sink/started", 1);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();

        LOGGER.info()
                .setMessage("Shutdown sink actor")
                .addData("actorRef", self())
                .addData("recordsWritten", _postRequests)
                .log();

        _periodicMetrics.recordCounter("actors/http_post_sink/stopped", 1);
    }

    @SuppressFBWarnings(value = "THROWS_METHOD_THROWS_CLAUSE_BASIC_EXCEPTION", justification = "Super method throws this")
    @Override
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
        super.preRestart(reason, message);

        _periodicMetrics.recordCounter("actors/http_post_sink/restarted", 1);
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
                .put("acceptedStatusCodes", _acceptedStatusCodes)
                .put("maximumConcurrency", _maximumConcurrency)
                .put("spreadingDelayMillis", _spreadingDelayMillis)
                .put("waiting", _waiting)
                .put("inflightRequestsCount", _inflightRequestsCount)
                .put("pendingRequestsCount", _pendingRequests.size())
                .put("periodicMetrics", _periodicMetrics)
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
                .match(PostSuccess.class, this::processSuccessRequest)
                .match(PostRejected.class, this::processRejectedRequest)
                .match(PostFailure.class, this::processFailedRequest)
                .match(WaitTimeExpired.class, this::waitTimeExpired)
                .match(SampleMetrics.class, this::sampleMetrics)
                .build();
    }

    private void sampleMetrics(final SampleMetrics ignored) {
        _periodicMetrics.recordGauge(_inflightRequestsCountName, _inflightRequestsCount);
        _periodicMetrics.recordGauge(_pendingRequestsQueueSizeName, _pendingRequests.size());
        _periodicMetrics.recordCounter(_samplesSentName, 0);
        _periodicMetrics.recordCounter(_evictedRequestsName, 0);
        _periodicMetrics.recordCounter(_requestSuccessName, 0);
        _periodicMetrics.recordCounter(_samplesDroppedName, 0);
    }

    private void waitTimeExpired(final WaitTimeExpired ignored) {
        LOGGER.debug()
                .setMessage("Received WaitTimeExpired message")
                .addContext("actor", self())
                .log();
        _waiting = false;
        dispatchPending();
    }

    private void processFailedRequest(final PostFailure failure) {
        _inflightRequestsCount--;
        POST_ERROR_LOGGER.error()
                .setMessage("Post error")
                .addData("sink", _sink)
                .addContext("actor", self())
                .setThrowable(failure.getCause())
                .log();
        dispatchPending();
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
        dispatchPending();
    }

    private void processRejectedRequest(final PostRejected rejected) {
        _postRequests++;
        _inflightRequestsCount--;
        final Response response = rejected.getResponse();
        final Optional<String> responseBody = Optional.ofNullable(response.getResponseBody());
        final byte[] requestBodyBytes = rejected.getRequest().getByteData();
        // CHECKSTYLE.OFF: IllegalInstantiation - This is ok for String from byte[]
        final String requestBody = requestBodyBytes == null ? null : new String(requestBodyBytes, StandardCharsets.UTF_8);
        // CHECKSTYLE.ON: IllegalInstantiation
        LOGGER.warn()
                .setMessage("Post rejected")
                .addData("sink", _sink)
                .addData("status", response.getStatusCode())
                .addData("request", requestBody)
                .addData("response", responseBody)
                .addContext("actor", self())
                .log();
        dispatchPending();
    }

    private void processEmitAggregation(final EmitAggregation emitMessage) {
        final PeriodicData periodicData = emitMessage.getData();

        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", _sink)
                .addData("dataSize", periodicData.getData().size())
                .addContext("actor", self())
                .log();
        _periodicMetrics.recordGauge(_inflightRequestsCountName, _inflightRequestsCount);

        if (!periodicData.getData().isEmpty()) {
            final Collection<RequestEntry.Builder> requestEntryBuilders = _sink.createRequests(_client, periodicData);
            final boolean pendingWasEmpty = _pendingRequests.isEmpty();

            final int evicted = Math.max(0, requestEntryBuilders.size() - _pendingRequests.remainingCapacity());
            for (final RequestEntry.Builder requestEntryBuilder : requestEntryBuilders) {
                // TODO(vkoskela): Add logging to client [MAI-89]
                // TODO(vkoskela): Add instrumentation to client [MAI-90]
                _pendingRequests.offer(requestEntryBuilder.setEnterTime(Instant.now()).build());
            }

            if (evicted > 0) {
                _periodicMetrics.recordCounter(_evictedRequestsName, evicted);
                EVICTED_LOGGER.warn()
                        .setMessage("Evicted data from HTTP sink queue")
                        .addData("sink", _sink)
                        .addData("count", evicted)
                        .addContext("actor", self())
                        .log();
            }

            _periodicMetrics.recordGauge(_pendingRequestsQueueSizeName, _pendingRequests.size());

            if (_spreadingDelayMillis > 0) {
                // If we don't currently have anything in-flight, we'll need to wait the spreading duration.
                if (!_waiting && pendingWasEmpty) {
                    _waiting = true;
                    LOGGER.debug()
                            .setMessage("Scheduling http requests for later transmission")
                            .addData("delayMs", _spreadingDelayMillis)
                            .addContext("actor", self())
                            .log();
                    context().system().scheduler().scheduleOnce(
                            FiniteDuration.apply(_spreadingDelayMillis, TimeUnit.MILLISECONDS),
                            self(),
                            WaitTimeExpired.getInstance(),
                            context().dispatcher(),
                            self());
                } else if (!_waiting) {
                   // If we have something in-flight continue to send without waiting
                   dispatchPending();
                }
                // Otherwise we're already waiting, these requests will be sent after the waiting is over, no need to do anything else.
            } else {
                // Spreading is disabled, just keep dispatching the work
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
        if (requestEntry == null) {
            return;
        }
        final long latencyInMillis = Duration.between(requestEntry.getEnterTime(), Instant.now()).toMillis();
        _periodicMetrics.recordTimer(_inQueueLatencyName, latencyInMillis, Optional.of(TimeUnit.MILLISECONDS));

        final Request request = requestEntry.getRequest();
        _inflightRequestsCount++;

        final CompletableFuture<Response> promise = new CompletableFuture<>();
        final long requestStartTime = System.currentTimeMillis();
        _client.executeRequest(request, new ResponseAsyncCompletionHandler(promise));
        final CompletionStage<Object> responsePromise = promise
                .handle((result, err) -> {
                    _periodicMetrics.recordTimer(
                            _requestLatencyName,
                            System.currentTimeMillis() - requestStartTime,
                            Optional.of(TimeUnit.MILLISECONDS));
                    final Object returnValue;
                    if (err == null) {
                        final int responseStatusCode = result.getStatusCode();
                        final int responseStatusClass = responseStatusCode / 100;
                        for (final int i : STATUS_CLASSES) {
                            _periodicMetrics.recordCounter(
                                    String.format("%s/%dxx", _responseStatusName, i),
                                    responseStatusClass == i ? 1 : 0);
                        }
                        if (_acceptedStatusCodes.contains(responseStatusCode)) {
                             returnValue =  new PostSuccess(result);
                            _periodicMetrics.recordCounter(_samplesSentName, requestEntry.getPopulationSize());
                        } else {
                             returnValue =  new PostRejected(request, result);
                            _periodicMetrics.recordCounter(_samplesDroppedName, requestEntry.getPopulationSize());
                        }
                    } else {
                        returnValue = new PostFailure(request, err);
                        _periodicMetrics.recordCounter(_samplesDroppedName, requestEntry.getPopulationSize());
                    }
                    _periodicMetrics.recordCounter(_requestSuccessName, (returnValue instanceof PostSuccess) ? 1 : 0);
                    return returnValue;
                });
        Patterns.pipe(responsePromise, context().dispatcher()).to(self());
    }

    private int _inflightRequestsCount = 0;
    private long _postRequests = 0;
    private boolean _waiting = false;
    private final int _maximumConcurrency;
    private final EvictingQueue<RequestEntry> _pendingRequests;
    private final AsyncHttpClient _client;
    private final HttpPostSink _sink;
    private final int _spreadingDelayMillis;
    private final PeriodicMetrics _periodicMetrics;
    private final ImmutableSet<Integer> _acceptedStatusCodes;

    private final String _evictedRequestsName;
    private final String _requestLatencyName;
    private final String _inQueueLatencyName;
    private final String _pendingRequestsQueueSizeName;
    private final String _inflightRequestsCountName;
    private final String _requestSuccessName;
    private final String _responseStatusName;
    private final String _samplesDroppedName;
    private final String _samplesSentName;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpPostSinkActor.class);
    private static final Logger POST_ERROR_LOGGER = LoggerFactory.getRateLimitLogger(HttpPostSinkActor.class, Duration.ofSeconds(30));
    private static final Logger EVICTED_LOGGER = LoggerFactory.getRateLimitLogger(HttpPostSinkActor.class, Duration.ofSeconds(30));
    private static final com.google.common.collect.ImmutableList<Integer> STATUS_CLASSES = ImmutableList.of(2, 3, 4, 5);

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
    private static final class WaitTimeExpired { 
        private static final WaitTimeExpired INSTANCE = new WaitTimeExpired();

        public static WaitTimeExpired getInstance() {
            return INSTANCE;
        }

        private WaitTimeExpired() {}
    }

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

    private static final class SampleMetrics {
        private SampleMetrics() { }
        private static final SampleMetrics INSTANCE = new SampleMetrics();
    }
}
