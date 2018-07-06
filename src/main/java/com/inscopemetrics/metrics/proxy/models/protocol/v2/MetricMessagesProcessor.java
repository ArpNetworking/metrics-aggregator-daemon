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
package com.inscopemetrics.metrics.proxy.models.protocol.v2;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.steno.LogValueMapFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.inscopemetrics.metrics.proxy.actors.Connection;
import com.inscopemetrics.metrics.proxy.models.messages.Command;
import com.inscopemetrics.metrics.proxy.models.messages.MetricReport;
import com.inscopemetrics.metrics.proxy.models.messages.MetricsList;
import com.inscopemetrics.metrics.proxy.models.messages.MetricsListRequest;
import com.inscopemetrics.metrics.proxy.models.messages.NewMetric;
import com.inscopemetrics.metrics.proxy.models.protocol.MessagesProcessor;

import java.util.Map;
import java.util.Set;

/**
 * Processes metric-based messages.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class MetricMessagesProcessor implements MessagesProcessor {
    /**
     * Public constructor.
     *
     * @param connection ConnectionContext where processing takes place
     * @param metrics {@link PeriodicMetrics} instance to record metrics to
     */
    public MetricMessagesProcessor(final Connection connection, final PeriodicMetrics metrics) {
        _connection = connection;
        _metrics = metrics;
    }

    @Override
    public boolean handleMessage(final Object message) {
        if (message instanceof Command) {
            //TODO(barp): Map with a POJO mapper [MAI-184]
            final Command command = (Command) message;
            final ObjectNode commandNode = (ObjectNode) command.getCommand();
            final String commandString = commandNode.get("command").asText();
            switch (commandString) {
                case COMMAND_GET_METRICS:
                    _connection.getTelemetry().tell(new MetricsListRequest(), _connection.getSelf());
                    break;
                case COMMAND_SUBSCRIBE_METRIC: {
                    _metrics.recordCounter(SUBSCRIBE_COUNTER, 1);
                    final String service = commandNode.get("service").asText();
                    final String metric = commandNode.get("metric").asText();
                    final String statistic = commandNode.get("statistic").asText();
                    _connection.subscribe(service, metric, statistic);
                    break;
                }
                case COMMAND_UNSUBSCRIBE_METRIC: {
                    _metrics.recordCounter(UNSUBSCRIBE_COUNTER, 1);
                    final String service = commandNode.get("service").asText();
                    final String metric = commandNode.get("metric").asText();
                    final String statistic = commandNode.get("statistic").asText();
                    _connection.unsubscribe(service, metric, statistic);
                    break;
                }
                default:
                    return false;
            }
        } else if (message instanceof NewMetric) {
            //TODO(barp): Map with a POJO mapper [MAI-184]
            _metrics.recordCounter(NEW_METRIC_COUNTER, 1);
            final NewMetric newMetric = (NewMetric) message;
            processNewMetric(newMetric);
        } else if (message instanceof MetricReport) {
            _metrics.recordCounter(REPORT_COUNTER, 1);
            final MetricReport report = (MetricReport) message;
            processMetricReport(report);
        } else if (message instanceof MetricsList) {
            _metrics.recordCounter(METRICS_LIST_COUNTER, 1);
            final MetricsList metricsList = (MetricsList) message;
            processMetricsList(metricsList);
        } else {
            return false;
        }
        return true;
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        // NOTE: Do not log connection context as this creates a circular reference
        return LogValueMapFactory.builder(this)
                .put("connection", _connection)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void processNewMetric(final NewMetric newMetric) {
        final ObjectNode n = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
        n.put("service", newMetric.getService());
        n.put("metric", newMetric.getMetric());
        n.put("statistic", newMetric.getStatistic());
        _connection.sendCommand(COMMAND_NEW_METRIC, n);
    }

    private void processMetricReport(final MetricReport report) {
        //TODO(barp): Map with a POJO mapper [MAI-184]
        final ObjectNode event = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
        event.put("server", report.getHost());
        event.put("service", report.getService());
        event.put("metric", report.getMetric());
        event.put("timestamp", report.getPeriodStart().toInstant().toEpochMilli());
        event.put("statistic", report.getStatistic());
        event.put("data", report.getValue());
        event.set("numeratorUnits", createUnitArrayNode(report.getNumeratorUnits()));
        event.set("denominatorUnits", createUnitArrayNode(report.getDenominatorUnits()));

        _connection.sendCommand(COMMAND_REPORT_METRIC, event);
    }

    private ArrayNode createUnitArrayNode(final ImmutableList<String> units) {
        final ArrayNode unitArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
        units.forEach(unitArrayNode::add);
        return unitArrayNode;
    }

    private void processMetricsList(final MetricsList metricsList) {
        //TODO(barp): Map with a POJO mapper [MAI-184]
        final ObjectNode dataNode = JsonNodeFactory.instance.objectNode();
        final ArrayNode services = JsonNodeFactory.instance.arrayNode();
        for (final Map.Entry<String, Map<String, Set<String>>> service : metricsList.getMetrics().entrySet()) {
            final ObjectNode serviceObject = JsonNodeFactory.instance.objectNode();
            serviceObject.put("name", service.getKey());
            final ArrayNode metrics = JsonNodeFactory.instance.arrayNode();
            for (final Map.Entry<String, Set<String>> metric : service.getValue().entrySet()) {
                final ObjectNode metricObject = JsonNodeFactory.instance.objectNode();
                metricObject.put("name", metric.getKey());
                final ArrayNode stats = JsonNodeFactory.instance.arrayNode();
                for (final String statistic : metric.getValue()) {
                    final ObjectNode statsObject = JsonNodeFactory.instance.objectNode();
                    statsObject.put("name", statistic);
                    statsObject.set("children", JsonNodeFactory.instance.arrayNode());
                    stats.add(statsObject);
                }
                metricObject.set("children", stats);
                metrics.add(metricObject);
            }
            serviceObject.set("children", metrics);
            services.add(serviceObject);
        }
        dataNode.set("metrics", services);
        _connection.sendCommand(COMMAND_METRICS_LIST, dataNode);
    }

    private final Connection _connection;
    private PeriodicMetrics _metrics;

    private static final String COMMAND_METRICS_LIST = "metricsList";
    private static final String COMMAND_REPORT_METRIC = "reportMetric";
    private static final String COMMAND_NEW_METRIC = "newMetric";
    private static final String COMMAND_SUBSCRIBE_METRIC = "subscribeMetric";
    private static final String COMMAND_UNSUBSCRIBE_METRIC = "unsubscribeMetric";
    private static final String COMMAND_GET_METRICS = "getMetrics";

    private static final String METRICS_PREFIX = "actors/connection/";
    private static final String METRICS_LIST_COUNTER = METRICS_PREFIX + "metrics_list";
    private static final String REPORT_COUNTER = METRICS_PREFIX + "metric_report";
    private static final String NEW_METRIC_COUNTER = METRICS_PREFIX + "new_metric";
    private static final String UNSUBSCRIBE_COUNTER = METRICS_PREFIX + "command/unsubscribe";
    private static final String SUBSCRIBE_COUNTER = METRICS_PREFIX + "command/subscribe";

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
}
