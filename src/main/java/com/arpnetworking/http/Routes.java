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

import com.arpnetworking.metrics.common.sources.ClientHttpSourceV1;
import com.arpnetworking.metrics.common.sources.ClientHttpSourceV2;
import com.arpnetworking.metrics.common.sources.ClientHttpSourceV3;
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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.ActorNotFound;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.PoisonPill;
import org.apache.pekko.actor.Status.Failure;
import org.apache.pekko.actor.Status.Success;
import org.apache.pekko.http.javadsl.model.ContentType;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpHeader;
import org.apache.pekko.http.javadsl.model.HttpMethods;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.model.headers.CacheControl;
import org.apache.pekko.http.javadsl.model.headers.CacheDirectives;
import org.apache.pekko.http.javadsl.model.ws.Message;
import org.apache.pekko.japi.JavaPartialFunction;
import org.apache.pekko.japi.function.Function;
import org.apache.pekko.pattern.Patterns;
import org.apache.pekko.stream.CompletionStrategy;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;
import org.apache.pekko.util.Timeout;

import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Http server routes.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
public final class Routes implements Function<HttpRequest, CompletionStage<HttpResponse>>,
        org.apache.pekko.japi.Function<HttpRequest, CompletionStage<HttpResponse>> {

    /**
     * Public constructor.
     *
     * @param actorSystem        Instance of {@link ActorSystem}.
     * @param metrics            Instance of {@link PeriodicMetrics}.
     * @param healthCheckPath    The path for the health check.
     * @param statusPath         The path for the status.
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

    @Override
    public CompletionStage<HttpResponse> apply(final HttpRequest request) {
        final Stopwatch requestTimer = Stopwatch.createStarted();
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
        return request.entity().toStrict(30000, _actorSystem)
                .thenApply(request::withEntity)
                .thenCompose(strictRequest -> {
                    final CompletionStage<HttpResponse> process = this.process(strictRequest);
                    process.whenComplete(
                            (response, failure) -> {
                                requestTimer.stop();

                                final int responseStatus;
                                if (response != null) {
                                    responseStatus = response.status().intValue();
                                } else {
                                    // TODO(ville): Figure out how to intercept post-exception mapping.
                                    responseStatus = 599;
                                }

                                _metrics.recordTimer(
                                        createMetricName(strictRequest, responseStatus, REQUEST_METRIC),
                                        requestTimer.elapsed(TimeUnit.NANOSECONDS),
                                        Optional.of(TimeUnit.NANOSECONDS));
                                _metrics.recordGauge(
                                        createMetricName(strictRequest, responseStatus, BODY_SIZE_METRIC),
                                        strictRequest.entity().getContentLengthOption().orElse(0));
                                final int responseStatusClass = responseStatus / 100;
                                for (final int i : STATUS_CLASSES) {
                                    _metrics.recordCounter(
                                            createMetricName(strictRequest, responseStatus, String.format("%s/%dxx", STATUS_METRIC, i)),
                                            responseStatusClass == i ? 1 : 0);
                                }

                                final LogBuilder log;
                                if (failure != null || responseStatusClass == 5) {
                                    log = LOGGER.info().setEvent("http.in.failure");
                                    if (failure != null) {
                                        log.setThrowable(failure);
                                    }
                                    if (!LOGGER.isTraceEnabled() && LOGGER.isInfoEnabled()) {
                                        log.addData("method", strictRequest.method().toString())
                                                .addData("url", strictRequest.getUri().toString())
                                                .addData(
                                                        "headers",
                                                        StreamSupport.stream(strictRequest.getHeaders().spliterator(), false)
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
                    return process;
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
            } else if (Objects.equals(path, APP_V3_SOURCE_PREFIX)) {
                return dispatchHttpRequest(request, ACTOR_APP_V3);
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
                // CHECKSTYLE.OFF: IllegalCatch - Pekko's functional interface declares Exception thrown
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
        final CompletionStage<ActorRef> refFuture =
                _actorSystem.actorSelection(actorName)
                        .resolveOne(Duration.ofSeconds(1));
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
        if (upgradeToWebSocketHeader.orElse(null) instanceof org.apache.pekko.http.impl.engine.ws.UpgradeToWebSocketLowLevel) {
            final org.apache.pekko.http.impl.engine.ws.UpgradeToWebSocketLowLevel lowLevelUpgradeToWebSocketHeader =
                    (org.apache.pekko.http.impl.engine.ws.UpgradeToWebSocketLowLevel) upgradeToWebSocketHeader.get();

            final ActorRef connection = _actorSystem.actorOf(Connection.props(_metrics, messageProcessorsFactory));
            final Sink<Message, ?> inChannel = Sink.actorRef(connection, PoisonPill.getInstance());
            final Source<Message, ActorRef> outChannel = Source.<Message>actorRef(
                            m -> m instanceof Success ? Optional.of(CompletionStrategy.draining()) : Optional.empty(),
                            m -> m instanceof Failure ? Optional.of(((Failure) m).cause()) : Optional.empty(),
                            TELEMETRY_BUFFER_SIZE,
                            OverflowStrategy.dropBuffer())
                    .mapMaterializedValue(channel -> {
                        _actorSystem.actorSelection("/user/telemetry").resolveOne(Timeout.apply(1, TimeUnit.SECONDS)).foreach(
                                new JavaPartialFunction<ActorRef, Object>() {
                                    @Override
                                    @Nullable
                                    public Object apply(final ActorRef telemetry, final boolean isCheck) {
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
        return (CompletionStage<T>) Patterns.ask(
                        _actorSystem.actorSelection(actorPath),
                        request,
                        Duration.ofSeconds(1))
                .exceptionally(throwable -> defaultValue);
    }

    private String createMetricName(final HttpRequest request, final int responseStatus, final String actionPart) {
        final StringBuilder nameBuilder = new StringBuilder()
                .append(REST_SERVICE_METRIC_ROOT)
                .append(request.method().value());
        if (responseStatus == StatusCodes.NOT_FOUND.intValue()) {
            nameBuilder.append("/unknown_route");
        } else {
            if (!request.getUri().path().startsWith("/")) {
                nameBuilder.append("/");
            }
            nameBuilder.append(request.getUri().path());
        }
        nameBuilder.append("/");
        nameBuilder.append(actionPart);
        return nameBuilder.toString();
    }

    @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
    private final transient ActorSystem _actorSystem;
    @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
    private final transient PeriodicMetrics _metrics;
    private final String _healthCheckPath;
    private final String _statusPath;
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
    private static final String APP_V3_SOURCE_PREFIX = "/metrics/v3/application";
    private static final String PROMETHEUS_SOURCE_PREFIX = "/metrics/prometheus";
    private static final String ACTOR_COLLECTD_V1 = "/user/" + CollectdHttpSourceV1.ACTOR_NAME;
    private static final String ACTOR_APP_V1 = "/user/" + ClientHttpSourceV1.ACTOR_NAME;
    private static final String ACTOR_APP_V2 = "/user/" + ClientHttpSourceV2.ACTOR_NAME;
    private static final String ACTOR_APP_V3 = "/user/" + ClientHttpSourceV3.ACTOR_NAME;
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

    @Serial
    private static final long serialVersionUID = 4336082511110058019L;

    static {
        String statusJson = "{}";
        try {
            statusJson = Resources.toString(Resources.getResource("status.json"), StandardCharsets.UTF_8);
            // CHECKSTYLE.OFF: IllegalCatch - Prevent program shutdown
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            LOGGER.error("Resource load failure; resource=status.json", e);
        }
        STATUS_JSON = statusJson;
    }

}
