/**
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
import akka.pattern.Patterns;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import akka.util.Timeout;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Timer;
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
import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

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
     * @param metricsFactory Instance of <code>MetricsFactory</code>.
     * @param healthCheckPath The path for the health check.
     * @param statusPath The path for the status.
     */
    public Routes(
            final ActorSystem actorSystem,
            final MetricsFactory metricsFactory,
            final String healthCheckPath,
            final String statusPath) {
        _actorSystem = actorSystem;
        _metricsFactory = metricsFactory;
        _healthCheckPath = healthCheckPath;
        _statusPath = statusPath;
    }

    /**
     * Creates a {@link Flow} based on executing the routes asynchronously.
     *
     * @return a new {@link Flow}
     */
    public Flow<HttpRequest, HttpResponse, NotUsed> flow() {
        return Flow.<HttpRequest>create()
                .mapAsync(1, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<HttpResponse> apply(final HttpRequest request) {
        final Metrics metrics = _metricsFactory.create();
        final Timer timer = metrics.createTimer(createTimerName(request));
        // TODO(vkoskela): Add a request UUID and include in MDC. [MAI-462]
        LOGGER.trace()
                .setEvent("http.in.start")
                .addData("method", request.method())
                .addData("url", request.getUri())
                .addData("headers", request.getHeaders())
                .log();
        return process(request).<HttpResponse>whenComplete(
                (response, failure) -> {
                    timer.close();
                    metrics.close();
                    final LogBuilder log = LOGGER.trace()
                            .setEvent("http.in")
                            .addData("method", request.method())
                            .addData("url", request.getUri())
                            .addData("status", response.status().intValue())
                            .addData("headers", request.getHeaders());
                    if (failure != null) {
                        log.setEvent("http.in.error").addData("exception", failure);
                    }
                    log.log();
                });
    }

    private CompletionStage<HttpResponse> process(final HttpRequest request) {
        final String path = request.getUri().path();
        if (HttpMethods.GET.equals(request.method())) {
            if (TELEMETRY_STREAM_V1_PATH.equals(path)) {
                return getHttpResponseForTelemetry(request, TELEMETRY_V1_FACTORY);
            } else if (TELEMETRY_STREAM_V2_PATH.equals(path)) {
                return getHttpResponseForTelemetry(request, TELEMETRY_V2_FACTORY);
            } else if (_healthCheckPath.equals(path)) {
                return ask("/user/status", Status.IS_HEALTHY, Boolean.FALSE)
                        .thenApply(
                                isHealthy -> HttpResponse.create()
                                        .withStatus(isHealthy ? StatusCodes.OK : StatusCodes.INTERNAL_SERVER_ERROR)
                                        .addHeader(PING_CACHE_CONTROL_HEADER)
                                        .withEntity(
                                                JSON_CONTENT_TYPE,
                                                ByteString.fromString(
                                                        "{\"status\":\""
                                                                + (isHealthy ? HEALTHY_STATE : UNHEALTHY_STATE)
                                                                + "\"}")));
            } else if (_statusPath.equals(path)) {
                return CompletableFuture.completedFuture(
                        HttpResponse.create()
                                .withStatus(StatusCodes.OK)
                                .withEntity(JSON_CONTENT_TYPE, ByteString.fromString(STATUS_JSON)));
            }
        }

        if (path.startsWith(SOURCE_PREFIX)) {
            final Future<ActorRef> refFuture = _actorSystem.actorSelection("/user" + path)
                    .resolveOne(FiniteDuration.create(1, TimeUnit.SECONDS));
            return FutureConverters.toJava(refFuture).thenCompose(
                    ref -> {
                        final CompletableFuture<HttpResponse> response = new CompletableFuture<>();
                        ref.tell(new RequestReply(request, response), ActorRef.noSender());
                        return response;
                    })
                    .exceptionally(err -> HttpResponse.create().withStatus(404));
        } else {
            return CompletableFuture.completedFuture(HttpResponse.create().withStatus(404));
        }
    }

    private CompletionStage<HttpResponse> getHttpResponseForTelemetry(
            final HttpRequest request,
            final MessageProcessorsFactory messageProcessorsFactory) {
        final Optional<HttpHeader> upgradeToWebSocketHeader = request.getHeader("UpgradeToWebSocket");
        if (upgradeToWebSocketHeader.orElse(null) instanceof akka.http.impl.engine.ws.UpgradeToWebSocketLowLevel) {
            final akka.http.impl.engine.ws.UpgradeToWebSocketLowLevel lowLevelUpgradeToWebSocketHeader =
                    (akka.http.impl.engine.ws.UpgradeToWebSocketLowLevel) upgradeToWebSocketHeader.get();

            final ActorRef connection = _actorSystem.actorOf(Connection.props(_metricsFactory, messageProcessorsFactory));
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

            final CompletionStage<HttpResponse> response = CompletableFuture.completedFuture(
                    lowLevelUpgradeToWebSocketHeader.handleMessagesWith(
                            inChannel,
                            outChannel));
            return response;
        }
        return CompletableFuture.completedFuture(HttpResponse.create().withStatus(StatusCodes.BAD_REQUEST));
    }

    @SuppressWarnings("unchecked")
    private <T> CompletionStage<T> ask(final String actorPath, final Object request, final T defaultValue) {
        return FutureConverters.toJava(
                (Future<T>) Patterns.ask(
                        _actorSystem.actorSelection(actorPath),
                        request,
                        Timeout.apply(1, TimeUnit.SECONDS)))
                .exceptionally(throwable -> defaultValue);
    }

    private String createTimerName(final HttpRequest request) {
        final StringBuilder nameBuilder = new StringBuilder()
                .append("rest_service/")
                .append(request.method().value());
        if (!request.getUri().path().startsWith("/")) {
            nameBuilder.append("/");
        }
        nameBuilder.append(request.getUri().path());
        return nameBuilder.toString();
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ActorSystem _actorSystem;
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final MetricsFactory _metricsFactory;
    private final String _healthCheckPath;
    private final String _statusPath;

    private static final Logger LOGGER = LoggerFactory.getLogger(Routes.class);

    // Telemetry
    private static final int TELEMETRY_BUFFER_SIZE = 256;
    private static final ProcessorsV1Factory TELEMETRY_V1_FACTORY = new ProcessorsV1Factory();
    private static final ProcessorsV2Factory TELEMETRY_V2_FACTORY = new ProcessorsV2Factory();
    private static final String TELEMETRY_STREAM_V1_PATH = "/telemetry/v1/stream";
    private static final String TELEMETRY_STREAM_V2_PATH = "/telemetry/v2/stream";
    private static final String SOURCE_PREFIX = "/source/";

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
    private static final ContentType TEXT_CONTENT_TYPE = ContentTypes.APPLICATION_JSON;

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
