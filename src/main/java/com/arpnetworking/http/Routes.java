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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.CacheControl;
import akka.http.javadsl.model.headers.CacheDirectives;
import akka.japi.JavaPartialFunction;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Timer;
import com.arpnetworking.metrics.mad.Status;
import com.arpnetworking.steno.LogBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.concurrent.Future;
import scala.runtime.AbstractFunction1;

import java.util.concurrent.TimeUnit;

/**
 * Http server routes.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
public final class Routes extends AbstractFunction1<HttpRequest, Future<HttpResponse>> {

    /**
     * Public constructor.
     *
     * @param actorSystem Instance of <code>ActorSystem</code>.
     * @param metricsFactory Instance of <code>MetricsFactory</code>.
     */
    public Routes(final ActorSystem actorSystem, final MetricsFactory metricsFactory) {
        _actorSystem = actorSystem;
        _metricsFactory = metricsFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<HttpResponse> apply(final HttpRequest request) {
        final Metrics metrics = _metricsFactory.create();
        final Timer timer = metrics.createTimer(createTimerName(request));
        // TODO(vkoskela): Add a request UUID and include in MDC. [MAI-462]
        LOGGER.trace()
                .setEvent("http.in.start")
                .addData("method", request.method())
                .addData("url", request.getUri())
                .addData("headers", request.getHeaders())
                .log();
        final Future<HttpResponse> futureResponse = process(request);
        futureResponse.onComplete(
                new OnComplete<HttpResponse>() {
                    @Override
                    public void onComplete(final Throwable failure, final HttpResponse response) {
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
                    }
                },
                _actorSystem.dispatcher());
        return futureResponse;
    }

    private Future<HttpResponse> process(final HttpRequest request) {
        if (HttpMethods.GET.equals(request.method())) {
            if (PING_PATH.equals(request.getUri().path())) {
                return ask("/user/status", Status.IS_HEALTHY, Boolean.FALSE)
                        .map(
                                new Mapper<Boolean, HttpResponse>() {
                                    @Override
                                    public HttpResponse apply(final Boolean isHealthy) {
                                        return HttpResponse.create()
                                                .withStatus(isHealthy ? StatusCodes.OK : StatusCodes.INTERNAL_SERVER_ERROR)
                                                .addHeader(PING_CACHE_CONTROL_HEADER)
                                                .withEntity(
                                                        JSON_CONTENT_TYPE,
                                                        "{\"status\":\""
                                                                + (isHealthy ? HEALTHY_STATE : UNHEALTHY_STATE)
                                                                + "\"}");
                                    }
                                },
                                _actorSystem.dispatcher()
                        );
            } else if (GROUPON_STATUS_PATH.equals(request.getUri().path())) {
                return Futures.successful(
                        HttpResponse.create()
                                .withStatus(StatusCodes.OK)
                                .withEntity(JSON_CONTENT_TYPE, GROUPON_STATUS_JSON));
            } else if (GROUPON_HEARTBEAT_PATH.equals(request.getUri().path())
                    || GROUPON_LEGACY_HEARTBEAT_PATH.equals(request.getUri().path())) {
                return ask("/user/status", Status.IS_HEALTHY, Boolean.FALSE)
                        .map(
                                new Mapper<Boolean, HttpResponse>() {
                                    @Override
                                    public HttpResponse apply(final Boolean isHealthy) {
                                        return HttpResponse.create()
                                                .withStatus(isHealthy ? StatusCodes.OK : StatusCodes.INTERNAL_SERVER_ERROR)
                                                .addHeader(PING_CACHE_CONTROL_HEADER)
                                                .withEntity(
                                                        TEXT_CONTENT_TYPE,
                                                        isHealthy ? HEALTHY_STATE : UNHEALTHY_STATE);
                                    }
                                },
                                _actorSystem.dispatcher()
                        );
            }
        }
        return Futures.successful(HttpResponse.create().withStatus(404));
    }

    private <T> Future<T> ask(final String actorPath, final Object request, final T defaultValue) {
        return _actorSystem.actorSelection(actorPath)
                .resolveOne(Timeout.apply(1, TimeUnit.SECONDS))
                .<T>flatMap(
                        new Mapper<ActorRef, Future<T>>() {
                            @Override
                            public Future<T> apply(final ActorRef actor) {
                                @SuppressWarnings("unchecked")
                                final Future<T> future = (Future<T>) Patterns.ask(
                                        actor,
                                        request,
                                        Timeout.apply(1, TimeUnit.SECONDS));
                                return future;
                            }
                        },
                        _actorSystem.dispatcher())
                .recover(
                        new JavaPartialFunction<Throwable, T>() {
                             @Override
                             public T apply(final Throwable t, final boolean isCheck) throws Exception {
                                 return defaultValue;
                             }
                         },
                        _actorSystem.dispatcher());
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

    private final ActorSystem _actorSystem;
    private final MetricsFactory _metricsFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(Routes.class);

    // Ping
    private static final String PING_PATH = "/ping";
    private static final HttpHeader PING_CACHE_CONTROL_HEADER = CacheControl.create(
            CacheDirectives.PRIVATE(),
            CacheDirectives.NO_CACHE,
            CacheDirectives.NO_STORE,
            CacheDirectives.MUST_REVALIDATE);
    private static final String UNHEALTHY_STATE = "UNHEALTHY";
    private static final String HEALTHY_STATE = "HEALTHY";

    // Groupon specific:
    private static final String GROUPON_STATUS_PATH = "/grpn/status.json";
    private static final String GROUPON_HEARTBEAT_PATH = "/grpn/healthcheck";
    private static final String GROUPON_LEGACY_HEARTBEAT_PATH = "/heartbeat.txt";

    private static final String GROUPON_STATUS_JSON;

    private static final ContentType JSON_CONTENT_TYPE = ContentType.create(MediaTypes.APPLICATION_JSON);
    private static final ContentType TEXT_CONTENT_TYPE = ContentType.create(MediaTypes.TEXT_PLAIN);

    static {
        String statusJson = "{}";
        try {
            statusJson = Resources.toString(Resources.getResource("status.json"), Charsets.UTF_8);
            // CHECKSTYLE.OFF: IllegalCatch - Prevent program shutdown
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            LOGGER.error("Resource load failure; resource=status.json", e);
        }
        GROUPON_STATUS_JSON = statusJson;
    }
}
