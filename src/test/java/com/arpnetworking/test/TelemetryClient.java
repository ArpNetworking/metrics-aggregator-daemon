/*
 * Copyright 2019 Dropbox.com
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
package com.arpnetworking.test;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Class which consumes streaming telemetry from MAD.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class TelemetryClient {

    /**
     * Retrieve the singleton instance of the {@code TelemetryClient}.
     *
     * @return the singleton instance of the {@code TelemetryClient}
     */
    public static synchronized TelemetryClient getInstance() {
        if (gTelemetryClient == null) {
            try {
                gTelemetryClient = new TelemetryClient(
                        getEnvOrDefault("MAD_HOST", "localhost"),
                        getEnvOrDefault("MAD_HTTP_PORT", DEFAULT_MAD_HTTP_PORT, Integer::parseInt));
                // CHECKSTYLE.OFF: IllegalCatch - Catastrophic failure
            } catch (final ExecutionException | InterruptedException e) {
                // CHECKSTYLE.ON: IllegalCatch
                throw new RuntimeException(e);
            }
        }
        return gTelemetryClient;
    }

    /**
     * Start receiving data for the specified time series.
     *
     * @param service the service name
     * @param metricName the metric name
     * @param statistic the statistic
     * @param callback the consumer to invoke with the received value(s)
     */
    public void subscribe(
            final String service,
            final String metricName,
            final Statistic statistic,
            final Consumer<Double> callback) {
        try {
            _webSocket.sendTextFrame(
                    OBJECT_MAPPER.writeValueAsString(
                            ImmutableMap.of(
                                    "command", "subscribeMetric",
                                    "service", service,
                                    "metric", metricName,
                                    "statistic", statistic.getName())));
            _callbacks.put(createKey(service, metricName, statistic), callback);
        } catch (final JsonProcessingException e) {
            LOGGER.warn()
                    .setMessage("Failed to subscribe to metric")
                    .setThrowable(e)
                    .log();
        }
    }

    /**
     * Stop receiving data for the specified time series.
     *
     * @param service the service name
     * @param metricName the metric name
     * @param statistic the statistic
     */
    public void unsubscribe(
            final String service,
            final String metricName,
            final Statistic statistic) {
        try {
            _callbacks.remove(createKey(service, metricName, statistic));
            _webSocket.sendTextFrame(
                    OBJECT_MAPPER.writeValueAsString(
                            ImmutableMap.of(
                                    "command", "unsubscribeMetric",
                                    "service", service,
                                    "metric", metricName,
                                    "statistic", statistic.getName())));
        } catch (final JsonProcessingException e) {
            LOGGER.warn()
                    .setMessage("Failed to unsubscribe to metric")
                    .setThrowable(e)
                    .log();
        }
    }

    private static String getEnvOrDefault(final String name, final String defaultValue) {
        return getEnvOrDefault(name, defaultValue, Function.identity());
    }

    private static <T> T getEnvOrDefault(final String name, final T defaultValue, final Function<String, T> map) {
        @Nullable final String value = System.getenv(name);
        return value == null ? defaultValue : map.apply(value);
    }

    private static String createKey(final String service, final String metric, final Statistic statistic) {
        return createKey(service, metric, statistic.getName());
    }

    private static String createKey(final String service, final String metric, final String statisticName) {
        return String.format("%s/%s/%s", service, metric, statisticName);
    }

    private TelemetryClient(final String host, final int port) throws ExecutionException, InterruptedException {
        final AsyncHttpClient asyncHttpClient = Dsl.asyncHttpClient();
        _webSocket = asyncHttpClient.prepareGet(String.format("ws://%s:%d/telemetry/v2/stream", host, port))
                .execute(
                        new WebSocketUpgradeHandler.Builder()
                                .addWebSocketListener(new TelemetryWebSocketListener(host, port, _callbacks))
                                .build())
                .get();

        _keepAliveExecutor = Executors.newScheduledThreadPool(1);
        _keepAliveExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        _webSocket.sendTextFrame(
                                OBJECT_MAPPER.writeValueAsString(
                                        Collections.singletonMap("command", "heartbeat")));
                    } catch (final JsonProcessingException e) {
                        LOGGER.warn()
                                .setMessage("Failed to send telemetry client heartbeat")
                                .setThrowable(e)
                                .log();
                    }
                },
                10,
                10,
                TimeUnit.SECONDS);
    }

    private final WebSocket _webSocket;
    private final ScheduledExecutorService _keepAliveExecutor;
    private final ConcurrentMap<String, Consumer<Double>> _callbacks = Maps.newConcurrentMap();

    private static TelemetryClient gTelemetryClient;
    private static final int DEFAULT_MAD_HTTP_PORT = 7090;
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryClient.class);

    private static class TelemetryWebSocketListener implements WebSocketListener {

        TelemetryWebSocketListener(
                final String host,
                final int port,
                final ConcurrentMap<String, Consumer<Double>> callbacks) {
            _host = host;
            _port = port;
            _callbacks = callbacks;
        }

        @Override
        public void onOpen(final WebSocket webSocket) {
            LOGGER.info()
                    .setMessage("Telemetry client: web socket open")
                    .addData("host", _host)
                    .addData("port", _port)
                    .log();
        }

        @Override
        public void onClose(final WebSocket webSocket, final int i, final String s) {
            LOGGER.info()
                    .setMessage("Telemetry client: web socket closed")
                    .addData("host", _host)
                    .addData("port", _port)
                    .log();
        }

        @Override
        public void onError(final Throwable throwable) {
            LOGGER.warn()
                    .setMessage("Telemetry client: web socket error")
                    .addData("host", _host)
                    .addData("port", _port)
                    .setThrowable(throwable)
                    .log();
        }

        @Override
        public void onTextFrame(final String payload, final boolean finalFragment, final int rsv) {
            _buffer.append(payload);
            if (finalFragment) {
                try {
                    handleMessage(OBJECT_MAPPER.readTree(_buffer.toString()));
                } catch (final IOException e) {
                    LOGGER.warn()
                            .setMessage("Failed to parse server message")
                            .addData("buffer", _buffer.toString())
                            .setThrowable(e)
                            .log();
                }
                _buffer.setLength(0);
            }
        }

        private void handleMessage(final JsonNode message) {
            if (message.get("response") != null) {
                if ("ok".equals(message.get("response").asText())) {
                    LOGGER.debug()
                            .setMessage("Telemetry client: received OK ping response")
                            .addData("host", _host)
                            .addData("port", _port)
                            .log();
                } else {
                    LOGGER.warn()
                            .setMessage("Telemetry client: received bad ping response")
                            .addData("host", _host)
                            .addData("port", _port)
                            .addData("message", message)
                            .log();
                }
            } else if (message.get("command") != null) {
                if ("reportMetric".equals(message.get("command").asText("unknown"))) {
                    final JsonNode data = message.get("data");
                    final String key = createKey(
                            data.get("service").asText(),
                            data.get("metric").asText(),
                            data.get("statistic").asText());
                    final Double value = data.get("data").doubleValue();
                    @Nullable final Consumer<Double> callback = _callbacks.get(key);
                    LOGGER.debug()
                            .setMessage("Telemetry client: received metric report")
                            .addData("host", _host)
                            .addData("port", _port)
                            .addData("key", key)
                            .addData("value", value)
                            .addData("callback", callback)
                            .log();
                    if (callback != null) {
                        callback.accept(value);
                    }
                }
            }
        }

        private final String _host;
        private final int _port;
        private final ConcurrentMap<String, Consumer<Double>> _callbacks;
        private final StringBuilder _buffer = new StringBuilder();
    }
}
