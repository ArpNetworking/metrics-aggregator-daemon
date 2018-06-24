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
package com.arpnetworking.metrics.proxy.models.protocol.v2;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.proxy.actors.Connection;
import com.arpnetworking.metrics.proxy.models.messages.Command;
import com.arpnetworking.metrics.proxy.models.messages.LogLine;
import com.arpnetworking.metrics.proxy.models.messages.LogReport;
import com.arpnetworking.metrics.proxy.models.messages.LogsList;
import com.arpnetworking.metrics.proxy.models.messages.LogsListRequest;
import com.arpnetworking.metrics.proxy.models.messages.NewLog;
import com.arpnetworking.metrics.proxy.models.protocol.MessagesProcessor;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Processes log-based messages.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class LogMessagesProcessor implements MessagesProcessor {
    /**
     * Public constructor.
     *
     * @param connection ConnectionContext where processing takes place
     * @param metrics {@link PeriodicMetrics} instance to record metrics to
     */
    public LogMessagesProcessor(final Connection connection, final PeriodicMetrics metrics) {
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
                case COMMAND_GET_LOGS:
                    _metrics.recordCounter(GET_LOGS_COUNTER, 1);
                    _connection.getTelemetry().tell(new LogsListRequest(), _connection.getSelf());
                    break;
                case COMMAND_SUBSCRIBE_LOG: {
                    _metrics.recordCounter(SUBSCRIBE_COUNTER, 1);
                    final Path log = Paths.get(commandNode.get("log").asText());
                    final ArrayNode regexes = commandNode.withArray("regexes");
                    subscribe(log, regexes);
                    break;
                }
                case COMMAND_UNSUBSCRIBE_LOG: {
                    _metrics.recordCounter(UNSUBSCRIBE_COUNTER, 1);
                    final Path log = Paths.get(commandNode.get("log").asText());
                    final ArrayNode regexes = commandNode.withArray("regexes");
                    unsubscribe(log, regexes);
                    break;
                }
                default:
                    return false;
            }
        } else if (message instanceof LogLine) {
            final LogLine logReport = (LogLine) message;
            processLogReport(logReport);
        } else if (message instanceof NewLog) {
            final NewLog newLog = (NewLog) message;
            processNewLog(newLog);
        } else if (message instanceof LogsList) {
            final LogsList logsList = (LogsList) message;
            processLogsList(logsList);

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
                .put("logsSubscriptions", _logsSubscriptions)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void processLogsList(final LogsList logsList) {
        _metrics.recordCounter(LOGS_LIST_COUNTER, 1);
        _connection.sendCommand(COMMAND_LOGS_LIST, OBJECT_MAPPER.convertValue(logsList, ObjectNode.class));
    }

    private void processLogReport(final LogLine rawReport) {
        _metrics.recordCounter(LOG_LINE_COUNTER, 1);
        final Path logFile = rawReport.getFile();

        final Set<String> regexes = _logsSubscriptions.get(logFile);
        if (regexes == null) {
            LOGGER.trace()
                    .setMessage("Not sending LogReport")
                    .addData("reason", "log not found in logsSubscriptions")
                    .addData("file", logFile)
                    .log();
            return;
        }

        if (regexes.isEmpty()) {
            LOGGER.trace()
                    .setMessage("Not sending LogReport")
                    .addData("reason", "log has not subscribed regexes")
                    .addData("file", logFile)
                    .log();
            return;
        }

        final String line = rawReport.convertLineToString();
        final List<String> matchingRegexes = new ArrayList<>();
        for (final String regex : regexes) {
            final Pattern pattern = PATTERNS_MAP.get(regex);
            if (pattern.matcher(line).matches()) {
                matchingRegexes.add(regex);
            }
        }

        if (matchingRegexes.size() > 0) {
            final LogReport logReport = new LogReport(
                    matchingRegexes,
                    logFile,
                    line,
                    extractTimestamp(line));
            _connection.sendCommand(COMMAND_REPORT_LOG, OBJECT_MAPPER.convertValue(logReport, ObjectNode.class));
        }
    }

    private void processNewLog(final NewLog newLog) {
        _metrics.recordCounter(NEW_LOG_COUNTER, 1);
        _connection.sendCommand(COMMAND_NEW_LOG, OBJECT_MAPPER.convertValue(newLog, ObjectNode.class));
    }

    private void subscribe(final Path log, final ArrayNode regexes) {
        if (!_logsSubscriptions.containsKey(log)) {
            _logsSubscriptions.put(log, Sets.<String>newHashSet());
        }
        final Set<String> logsRegexes = _logsSubscriptions.get(log);
        for (JsonNode node : regexes) {
            final String regex = node.asText();
            logsRegexes.add(regex);
            if (!PATTERNS_MAP.containsKey(regex)) {
                PATTERNS_MAP.put(regex, Pattern.compile(regex));
            }
        }
    }

    private void unsubscribe(final Path log, final ArrayNode regexes) {
        if (!_logsSubscriptions.containsKey(log)) {
            return;
        }
        final Set<String> logsRegexes = _logsSubscriptions.get(log);
        for (JsonNode node : regexes) {
            logsRegexes.remove(node.asText());
        }
    }

    private ZonedDateTime extractTimestamp(final String line) {
        // TODO(vkoskela): Implement different timestamp extract strategies [MAI-409]
        return ZonedDateTime.now();
    }

    private final Map<Path, Set<String>> _logsSubscriptions = Maps.newHashMap();
    private final Connection _connection;
    private PeriodicMetrics _metrics;

    private static final Map<String, Pattern> PATTERNS_MAP = Maps.newHashMap();
    private static final String COMMAND_GET_LOGS = "getLogs";
    private static final String COMMAND_SUBSCRIBE_LOG = "subscribeLog";
    private static final String COMMAND_UNSUBSCRIBE_LOG = "unsubscribeLog";
    private static final String COMMAND_LOGS_LIST = "logsList";
    private static final String COMMAND_REPORT_LOG = "reportLog";
    private static final String COMMAND_NEW_LOG = "newLog";

    private static final String METRICS_PREFIX = "message_processor/log/";
    private static final String LOG_LINE_COUNTER = METRICS_PREFIX + "log_line";
    private static final String LOGS_LIST_COUNTER = METRICS_PREFIX + "list_log";
    private static final String NEW_LOG_COUNTER = METRICS_PREFIX + "new_log";
    private static final String LOG_REPORT_COUNTER = METRICS_PREFIX + "log_report";
    private static final String SUBSCRIBE_COUNTER = METRICS_PREFIX + "subscribe";
    private static final String UNSUBSCRIBE_COUNTER = METRICS_PREFIX + "unsubscribe";
    private static final String GET_LOGS_COUNTER = METRICS_PREFIX + "command/get_logs";

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMessagesProcessor.class);
}
