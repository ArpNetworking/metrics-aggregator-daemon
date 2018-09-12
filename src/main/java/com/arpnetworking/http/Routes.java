/*
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.http;

import akka.NotUsed;
import akka.actor.ActorNotFound;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.CacheControl;
import akka.http.javadsl.model.headers.CacheDirectives;
import akka.http.javadsl.model.ws.Message;
import akka.japi.JavaPartialFunction;
import akka.japi.function.Function;
import akka.pattern.PatternsCS;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import akka.util.Timeout;
import com.arpnetworking.metrics.Units;
import com.arpnetworking.metrics.common.sources.ClientHttpSourceV1;
import com.arpnetworking.metrics.common.sources.ClientHttpSourceV2;
import com.arpnetworking.metrics.common.sources.CollectdHttpSourceV1;
import com.arpnetworking.metrics.common.sources.PrometheusHttpSource;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.actors.Status;
import com.arpnetworking.metrics.proxy.actors.Connection;
import com.arpnetworking.metrics.proxy.models.messages.Connect;
import com.arpnetworking.metrics.proxy.models.protocol.MessageProcessorsFactory;
import com.arpnetworking.metrics.proxy.models.protocol.v1.ProcessorsV1Factory;
import com.arpnetworking.metrics.proxy.models.protocol.v2.ProcessorsV2Factory;
import com.arpnetworking.steno.LogBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.concurrent.duration.FiniteDuration;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Http server routes.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
public final class Routes implements Function<HttpRequest, CompletionStage<HttpResponse>> {

    /**
     * Public constructor.
     *
     * @param actorSystem Instance of <code>ActorSystem</code>.
     * @param metrics Instance of <code>PeriodicMetrics</code>.
     * @param healthCheckPath The path for the health check.
     * @param statusPath The path for the status.
     * @param supplementalRoutes List of supplemental routes in priority order.
     */
    public Routes(
            final ActorSystem actorSystem,
            final PeriodicMetrics metrics,
            final String healthCheckPath,
            final String statusPath,
            final ImmutableList<SupplementalRoutes> supplementalRoutes) {
        _actorSystem = actorSystem;
        _metrics = metrics;
        _healthCheckPath = healthCheckPath;
        _statusPath = statusPath;
        _supplementalRoutes = supplementalRoutes;
    }

    /**
     * Creates a {@link Flow} based on executing the routes asynchronously.
     *
     * @return a new {@link Flow}
     */
    public Flow<HttpRequest, HttpResponse, NotUsed> flow() {
        return Flow.<HttpRequest>create()
                .mapAsync(1, this)
                .recoverWithRetries(
                        0,
                        Exception.class,
                        () -> Source.single(HttpResponse.create().withStatus(StatusCodes.INTERNAL_SERVER_ERROR)));
    }

    @Override
    public CompletionStage<HttpResponse> apply(final HttpRequest request) {
        final Stopwatch requestTimer = Stopwatch.createStarted();
        _metrics.recordGauge(
                createMetricName(request, BODY_SIZE_METRIC),
                request.entity().getContentLengthOption().orElse(0L),
                Optional.of(Units.BYTE));
        final UUID requestId = UUID.randomUUID();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace()
                    .setEvent("http.in.start")
                    .addContext("requestId", requestId)
                    .addData("method", request.method().toString())
                    .addData("url", request.getUri().toString())
                    .addData(
                            "headers",
                            StreamSupport.stream(request.getHeaders().spliterator(), false)
                                    .map(h -> h.name() + "=" + h.value())
                                    .collect(Collectors.toList()))
                    .log();
        }
        // TODO(ville): Add a request UUID and include in MDC.
        LOGGER.trace()
                .setEvent("http.in.start")
                .addData("method", request.method())
                .addData("url", request.getUri())
                .addData("headers", request.getHeaders())
                .log();
        return process(request).whenComplete(
                (response, failure) -> {
                    requestTimer.stop();
                    _metrics.recordTimer(
                            createMetricName(request, REQUEST_METRIC),
                            requestTimer.elapsed(TimeUnit.NANOSECONDS),
                            Optional.of(Units.NANOSECOND));

                    final int responseStatus;
                    if (response != null) {
                        responseStatus = response.status().intValue();
                    } else {
                        // TODO(ville): Figure out how to intercept post-exception mapping.
                        responseStatus = 599;
                    }
                    final int responseStatusClass = responseStatus / 100;
                    for (final int i : STATUS_CLASSES) {
                        _metrics.recordCounter(
                                createMetricName(request, String.format("%s/%dxx", STATUS_METRIC, i)),
                                responseStatusClass == i ? 1 : 0);
                    }

                    final LogBuilder log;
                    if (failure != null || responseStatusClass == 5) {
                        log = LOGGER.info().setEvent("http.in.failure");
                        if (failure != null) {
                            log.setThrowable(failure);
                        }
                        if (!LOGGER.isTraceEnabled() && LOGGER.isInfoEnabled()) {
                            log.addData("method", request.method().toString())
                                    .addData("url", request.getUri().toString())
                                    .addData(
                                            "headers",
                                            StreamSupport.stream(request.getHeaders().spliterator(), false)
                                                    .map(h -> h.name() + "=" + h.value())
                                                    .collect(Collectors.toList()));
                        }
                    } else {
                        log = LOGGER.trace().setEvent("http.in.complete");
                    }
                    log.addContext("requestId", requestId)
                            .addData("status", responseStatus)
                            .log();
                });
    }

    private CompletionStage<HttpResponse> process(final HttpRequest request) {
        final String path = request.getUri().path();
        if (Objects.equals(HttpMethods.GET, request.method())) {
            if (Objects.equals(TELEMETRY_STREAM_V1_PATH, path)) {
                return getHttpResponseForTelemetry(request, TELEMETRY_V1_FACTORY);
            } else if (Objects.equals(TELEMETRY_STREAM_V2_PATH, path)) {
                return getHttpResponseForTelemetry(request, TELEMETRY_V2_FACTORY);
            } else if (Objects.equals(_healthCheckPath, path)) {
                return ask("/user/status", Status.IS_HEALTHY, Boolean.FALSE)
                        .thenApply(
                                isHealthy -> HttpResponse.create()
                                        .withStatus(isHealthy ? StatusCodes.OK : StatusCodes.INTERNAL_SERVER_ERROR)
                                        .addHeader(PING_CACHE_CONTROL_HEADER)
                                        .withEntity(
                                                JSON_CONTENT_TYPE,
                                                ByteString.fromString(
                                                        // TODO(barp): Don't use strings to build json
                                                        "{\"status\":\""
                                                                + (isHealthy ? HEALTHY_STATE : UNHEALTHY_STATE)
                                                                + "\"}")));
            } else if (Objects.equals(_statusPath, path)) {
                return CompletableFuture.completedFuture(
                        HttpResponse.create()
                                .withStatus(StatusCodes.OK)
                                .withEntity(JSON_CONTENT_TYPE, ByteString.fromString(STATUS_JSON)));
            }
        } else if (Objects.equals(HttpMethods.POST, request.method())) {
            if (Objects.equals(path, COLLECTD_V1_SOURCE_PREFIX)) {
                return dispatchHttpRequest(request, ACTOR_COLLECTD_V1);
            } else if (Objects.equals(path, APP_V2_SOURCE_PREFIX)) {
                return dispatchHttpRequest(request, ACTOR_APP_V2);
            } else if (Objects.equals(path, APP_V1_SOURCE_PREFIX)) {
                return dispatchHttpRequest(request, ACTOR_APP_V1);
            } else if (Objects.equals(path, PROMETHEUS_SOURCE_PREFIX)) {
                return dispatchHttpRequest(request, ACTOR_PROMETHEUS);
            }
        }

        for (final SupplementalRoutes supplementalRoutes : _supplementalRoutes) {
            try {
                final Optional<CompletionStage<HttpResponse>> supplmentalRouteFuture =
                        supplementalRoutes.apply(request);
                if (supplmentalRouteFuture.isPresent()) {
                    return supplmentalRouteFuture.get();
                }
                // CHECKSTYLE.OFF: IllegalCatch - Akka's functional interface declares Exception thrown
            } catch (final Exception e) {
                // CHECKSTYLE.ON: IllegalCatch
                LOGGER.warn()
                        .setMessage("Supplemental routes threw an exception")
                        .addData("supplementalRoutes", supplementalRoutes)
                        .setThrowable(e)
                        .log();
                break;
            }
        }

        return CompletableFuture.completedFuture(HttpResponse.create().withStatus(StatusCodes.NOT_FOUND));
    }

    private CompletionStage<HttpResponse> dispatchHttpRequest(final HttpRequest request, final String actorName) {
        final CompletionStage<ActorRef> refFuture = _actorSystem.actorSelection(actorName)
                .resolveOneCS(FiniteDuration.create(1, TimeUnit.SECONDS));
        return refFuture.thenCompose(
                ref -> {
                    final CompletableFuture<HttpResponse> response = new CompletableFuture<>();
                    ref.tell(new RequestReply(request, response), ActorRef.noSender());
                    return response;
                })
                // We return 404 here since actor startup is controlled by config and
                // the actors may not be running.
                .exceptionally(err -> {
                    final Throwable cause = err.getCause();
                    if (cause instanceof ActorNotFound) {
                        return HttpResponse.create().withStatus(StatusCodes.NOT_FOUND);
                    }
                    LOGGER.error()
                            .setMessage("Unhandled exception when looking up actor for http request routing")
                            .addData("actorName", actorName)
                            .setThrowable(cause)
                            .log();
                    return HttpResponse.create().withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
                });
    }

    private CompletionStage<HttpResponse> getHttpResponseForTelemetry(
            final HttpRequest request,
            final MessageProcessorsFactory messageProcessorsFactory) {
        final Optional<HttpHeader> upgradeToWebSocketHeader = request.getHeader("UpgradeToWebSocket");
        if (upgradeToWebSocketHeader.orElse(null) instanceof akka.http.impl.engine.ws.UpgradeToWebSocketLowLevel) {
            final akka.http.impl.engine.ws.UpgradeToWebSocketLowLevel lowLevelUpgradeToWebSocketHeader =
                    (akka.http.impl.engine.ws.UpgradeToWebSocketLowLevel) upgradeToWebSocketHeader.get();

            final ActorRef connection = _actorSystem.actorOf(Connection.props(_metrics, messageProcessorsFactory));
            final Sink<Message, ?> inChannel = Sink.actorRef(connection, PoisonPill.getInstance());
            final Source<Message, ActorRef> outChannel = Source.<Message>actorRef(TELEMETRY_BUFFER_SIZE, OverflowStrategy.dropBuffer())
                    .<ActorRef>mapMaterializedValue(channel -> {
                        _actorSystem.actorSelection("/user/telemetry").resolveOne(Timeout.apply(1, TimeUnit.SECONDS)).onSuccess(
                                new JavaPartialFunction<ActorRef, Object>() {
                                    @Override
                                    public Object apply(final ActorRef telemetry, final boolean isCheck) throws Exception {
                                        final Connect connectMessage = new Connect(telemetry, connection, channel);
                                        connection.tell(connectMessage, ActorRef.noSender());
                                        telemetry.tell(connectMessage, ActorRef.noSender());
                                        return null;
                                    }
                                },
                                _actorSystem.dispatcher()
                        );
                        return channel;
                    });

            return CompletableFuture.completedFuture(
                    lowLevelUpgradeToWebSocketHeader.handleMessagesWith(
                            inChannel,
                            outChannel));
        }
        return CompletableFuture.completedFuture(HttpResponse.create().withStatus(StatusCodes.BAD_REQUEST));
    }

    @SuppressWarnings("unchecked")
    private <T> CompletionStage<T> ask(final String actorPath, final Object request, final T defaultValue) {
        return (CompletionStage<T>) PatternsCS.ask(
                        _actorSystem.actorSelection(actorPath),
                        request,
                        Timeout.apply(1, TimeUnit.SECONDS))
                .exceptionally(throwable -> defaultValue);
    }

    private String createMetricName(final HttpRequest request, final String actionPart) {
        final StringBuilder nameBuilder = new StringBuilder()
                .append(REST_SERVICE_METRIC_ROOT)
                .append(request.method().value());
        if (!request.getUri().path().startsWith("/")) {
            nameBuilder.append("/");
        }
        nameBuilder.append(request.getUri().path());
        nameBuilder.append("/");
        nameBuilder.append(actionPart);
        return nameBuilder.toString();
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ActorSystem _actorSystem;
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final PeriodicMetrics _metrics;
    private final String _healthCheckPath;
    private final String _statusPath;
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ImmutableList<SupplementalRoutes> _supplementalRoutes;

    private static final Logger LOGGER = LoggerFactory.getLogger(Routes.class);

    // Telemetry
    private static final int TELEMETRY_BUFFER_SIZE = 256;
    private static final ProcessorsV1Factory TELEMETRY_V1_FACTORY = new ProcessorsV1Factory();
    private static final ProcessorsV2Factory TELEMETRY_V2_FACTORY = new ProcessorsV2Factory();
    private static final String TELEMETRY_STREAM_V1_PATH = "/telemetry/v1/stream";
    private static final String TELEMETRY_STREAM_V2_PATH = "/telemetry/v2/stream";
    private static final String COLLECTD_V1_SOURCE_PREFIX = "/metrics/v1/collectd";
    private static final String APP_V1_SOURCE_PREFIX = "/metrics/v1/application";
    private static final String APP_V2_SOURCE_PREFIX = "/metrics/v2/application";
    private static final String PROMETHEUS_SOURCE_PREFIX = "/metrics/prometheus/application";
    private static final String ACTOR_COLLECTD_V1 = "/user/" + CollectdHttpSourceV1.ACTOR_NAME;
    private static final String ACTOR_APP_V1 = "/user/" + ClientHttpSourceV1.ACTOR_NAME;
    private static final String ACTOR_APP_V2 = "/user/" + ClientHttpSourceV2.ACTOR_NAME;
    private static final String ACTOR_PROMETHEUS = "/user/" + PrometheusHttpSource.ACTOR_NAME;
    private static final String REST_SERVICE_METRIC_ROOT = "rest_service/";
    private static final String BODY_SIZE_METRIC = "body_size";
    private static final String REQUEST_METRIC = "request";
    private static final String STATUS_METRIC = "status";
    private static final ImmutableList<Integer> STATUS_CLASSES = ImmutableList.of(2, 3, 4, 5);

    // Ping
    private static final HttpHeader PING_CACHE_CONTROL_HEADER = CacheControl.create(
            CacheDirectives.PRIVATE(),
            CacheDirectives.NO_CACHE,
            CacheDirectives.NO_STORE,
            CacheDirectives.MUST_REVALIDATE);
    private static final String UNHEALTHY_STATE = "UNHEALTHY";
    private static final String HEALTHY_STATE = "HEALTHY";
    private static final String STATUS_JSON;

    private static final ContentType JSON_CONTENT_TYPE = ContentTypes.APPLICATION_JSON;

    private static final long serialVersionUID = 4336082511110058019L;

    static {
        String statusJson = "{}";
        try {
            statusJson = Resources.toString(Resources.getResource("status.json"), Charsets.UTF_8);
            // CHECKSTYLE.OFF: IllegalCatch - Prevent program shutdown
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            LOGGER.error("Resource load failure; resource=status.json", e);
        }
        STATUS_JSON = statusJson;
    }

}
