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
package com.arpnetworking.metrics.proxy.actors;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.proxy.models.messages.Command;
import com.arpnetworking.metrics.proxy.models.messages.Connect;
import com.arpnetworking.metrics.proxy.models.messages.MetricReport;
import com.arpnetworking.metrics.proxy.models.protocol.MessageProcessorsFactory;
import com.arpnetworking.metrics.proxy.models.protocol.MessagesProcessor;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.PoisonPill;
import org.apache.pekko.actor.Props;
import org.apache.pekko.http.javadsl.model.ws.Message;
import org.apache.pekko.http.javadsl.model.ws.TextMessage;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Actor class to hold the state for a single connection.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class Connection extends AbstractActor {

    /**
     * Public constructor.
     *
     * @param metrics Instance of {@link PeriodicMetrics}.
     * @param processorsFactory Factory for producing the protocol's {@link MessagesProcessor}
     */
    public Connection(
            final PeriodicMetrics metrics,
            final MessageProcessorsFactory processorsFactory) {
        _metrics = metrics;
        _processorsFactory = processorsFactory;
    }

    /**
     * Factory for creating a {@link Props} with strong typing.
     *
     * @param metrics Instance of {@link PeriodicMetrics}.
     * @param messageProcessorsFactory Factory to create a {@link MessagesProcessor} object.
     * @return a new Props object to create a {@link Connection} actor.
     */
    public static Props props(
            final PeriodicMetrics metrics,
            final MessageProcessorsFactory messageProcessorsFactory) {
        return Props.create(
                Connection.class,
                metrics,
                messageProcessorsFactory);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        _messageProcessors = _processorsFactory.create(this, _metrics);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Connect.class, connect -> {
                    LOGGER.info()
                            .setMessage("Connected stream")
                            .addData("actor", self())
                            .addData("data", connect)
                            .log();

                    _telemetry = connect.getTelemetry();
                    _channel = connect.getChannel();
                })
                .match(org.apache.pekko.actor.Status.Failure.class, message -> {
                    // This message is sent by the incoming stream when there is a failure
                    // in the stream (see org.apache.pekko.stream.javadsl.Sink.scala).
                    LOGGER.info()
                            .setMessage("Closing stream")
                            .addData("actor", self())
                            .addData("data", message)
                            .log();
                    getSelf().tell(PoisonPill.getInstance(), getSelf());
                })
                .match(PeriodicData.class, message -> {
                    processPeriodicData(message);
                })
                .matchAny(message -> {
                    if (_channel == null) {
                        LOGGER.warn()
                                .setMessage("Unable to process message")
                                .addData("reason", "channel actor not materialized")
                                .addData("actor", self())
                                .addData("data", message)
                                .log();
                        return;
                    }

                    boolean messageProcessed = false;
                    final Object command;
                    if (message instanceof Message) {
                        // TODO(vkoskela): Handle streamed text (e.g. non-strict)
                        command = new Command(OBJECT_MAPPER.readTree(((Message) message).asTextMessage().getStrictText()));
                    } else {
                        command = message;
                    }
                    for (final MessagesProcessor messagesProcessor : _messageProcessors) {
                        messageProcessed = messagesProcessor.handleMessage(command);
                        if (messageProcessed) {
                            break;
                        }
                    }
                    if (!messageProcessed) {
                        _metrics.recordCounter(UNKNOWN_COUNTER, 1);
                        if (message instanceof Command) {
                            _metrics.recordCounter(UNKNOWN_COMMAND_COUNTER, 1);
                            LOGGER.warn()
                                    .setMessage("Unable to process message")
                                    .addData("reason", "unsupported command")
                                    .addData("actor", self())
                                    .addData("data", message)
                                    .log();
                            unhandled(message);
                        } else {
                            _metrics.recordCounter("Actors/Connection/UNKNOWN", 1);
                            LOGGER.warn()
                                    .setMessage("Unable to process message")
                                    .addData("reason", "unsupported message")
                                    .addData("actor", self())
                                    .addData("data", message)
                                    .log();
                            unhandled(message);
                        }
                    }
                })
            .build();
    }

    /**
     * Sends a json object to the connected client.
     *
     * @param message The message to send.
     */
    public void send(final ObjectNode message) {
        try {
            _channel.tell(TextMessage.create(OBJECT_MAPPER.writeValueAsString(message)), self());
        } catch (final JsonProcessingException e) {
            LOGGER.error()
                    .setMessage("Unable to send message")
                    .addData("reason", "serialization exception")
                    .addData("actor", self())
                    .addData("data", message)
                    .setThrowable(e)
                    .log();
        }
    }

    /**
     * Sends a command object to the connected client.
     *
     * @param command The command.
     * @param data The data for the command.
     */
    public void sendCommand(final String command, final ObjectNode data) {
        final ObjectNode message = JsonNodeFactory.instance.objectNode();
        message.put("command", command);
        message.set("data", data);
        send(message);
    }

    /**
     * Subscribe the connection to a stream.
     *
     * @param service The service to subscribe to.
     * @param metric The metric to subscribe to.
     * @param statistic The statistic to subscribe to.
     */
    public void subscribe(final String service, final String metric, final String statistic) {
        if (!_subscriptions.containsKey(service)) {
            _subscriptions.put(service, Maps.newHashMap());
        }

        final Map<String, Set<String>> metrics = _subscriptions.get(service);
        if (!metrics.containsKey(metric)) {
            metrics.put(metric, Sets.newHashSet());
        }

        final Set<String> statistics = metrics.get(metric);
        if (!statistics.contains(statistic)) {
            statistics.add(statistic);
        }
    }

    /**
     * Unsubscribe the connection from a stream.
     *
     * @param service The service to unsubscribe from.
     * @param metric The metric to unsubscribe from.
     * @param statistic The statistic to unsubscribe from.
     */
    public void unsubscribe(final String service, final String metric, final String statistic) {
        if (!_subscriptions.containsKey(service)) {
            return;
        }

        final Map<String, Set<String>> metrics = _subscriptions.get(service);
        if (!metrics.containsKey(metric)) {
            return;
        }

        final Set<String> statistics = metrics.get(metric);
        if (statistics.contains(statistic)) {
            statistics.remove(statistic);
        }
    }

    /**
     * Accessor to this Connection's Telemetry actor.
     *
     * @return This Connection's Telemetry actor.
     */
    public ActorRef getTelemetry() {
        return _telemetry;
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("connection", _channel)
                .put("messageProcessors", _messageProcessors)
                .put("subscriptions", _subscriptions)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void processPeriodicData(final PeriodicData message) {
        final Key dimensions = message.getDimensions();
        final String host = dimensions.getHost();
        final String service = dimensions.getService();
        final Map<String, Set<String>> metrics = _subscriptions.get(service);
        if (metrics == null) {
            LOGGER.trace()
                    .setMessage("Not sending MetricReport")
                    .addData("reason", "service not found in subscriptions")
                    .addData("service", service)
                    .log();
            return;
        }

        for (final Map.Entry<String, AggregatedData> entry : message.getData().entries()) {
            final String metric = entry.getKey();
            final AggregatedData datum = entry.getValue();

            final Set<String> stats = metrics.get(metric);
            if (stats == null) {
                LOGGER.trace()
                        .setMessage("Not sending MetricReport")
                        .addData("reason", "metric not found in subscriptions")
                        .addData("metric", metric)
                        .log();
                continue;
            }

            final String statisticName = datum.getStatistic().getName();
            if (!stats.contains(statisticName)) {
                LOGGER.trace()
                        .setMessage("Not sending MetricReport")
                        .addData("reason", "statistic not found in subscriptions")
                        .addData("statistic", statisticName)
                        .log();
                continue;
            }

            final MetricReport metricReport = new MetricReport(
                    service,
                    host,
                    statisticName,
                    metric,
                    datum.getValue().getValue(),
                    datum.getValue().getUnit(),
                    message.getStart());
            for (final MessagesProcessor messagesProcessor : _messageProcessors) {
                if (messagesProcessor.handleMessage(metricReport)) {
                    break;
                }
            }
        }
    }

    private ActorRef _telemetry;
    private ActorRef _channel;

    private final PeriodicMetrics _metrics;
    private final MessageProcessorsFactory _processorsFactory;
    private List<MessagesProcessor> _messageProcessors;
    private final Map<String, Map<String, Set<String>>> _subscriptions = Maps.newHashMap();

    private static final String METRICS_PREFIX = "actors/connection/";
    private static final String UNKNOWN_COMMAND_COUNTER = METRICS_PREFIX + "command/UNKNOWN";
    private static final String UNKNOWN_COUNTER = METRICS_PREFIX + "UNKNOWN";

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
}
