/*
 * Copyright 2014 Brandon Arp
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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.proxy.models.messages.Connect;
import com.arpnetworking.metrics.proxy.models.messages.LogFileAppeared;
import com.arpnetworking.metrics.proxy.models.messages.LogFileDisappeared;
import com.arpnetworking.metrics.proxy.models.messages.LogLine;
import com.arpnetworking.metrics.proxy.models.messages.LogsList;
import com.arpnetworking.metrics.proxy.models.messages.LogsListRequest;
import com.arpnetworking.metrics.proxy.models.messages.MetricsList;
import com.arpnetworking.metrics.proxy.models.messages.MetricsListRequest;
import com.arpnetworking.metrics.proxy.models.messages.NewLog;
import com.arpnetworking.metrics.proxy.models.messages.NewMetric;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Cancellable;
import org.apache.pekko.actor.Terminated;
import org.apache.pekko.dispatch.ExecutionContexts;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Actor responsible for holding the set of connected websockets and publishing
 * metrics to them.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 * @author Mohammed Kamel (mkamel at groupon dot com)
 */
public class Telemetry extends AbstractActor {

    /**
     * Public constructor.
     *
     * @param metricsFactory Instance of {@link MetricsFactory}.
     */
    @Inject
    public Telemetry(final MetricsFactory metricsFactory) {
        _metricsFactory = metricsFactory;
        _metrics = metricsFactory.create();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("instrument", message -> periodicInstrumentation())
                .match(PeriodicData.class, this::executePeriodicData)
                .match(Connect.class, this::executeConnect)
                .match(LogLine.class, this::executeLogLine)
                .match(MetricsListRequest.class, ignored -> executeMetricsListRequest())
                .match(LogsListRequest.class, ignored -> executeLogsListRequest())
                .match(LogFileAppeared.class, this::executeLogAdded)
                .match(LogFileDisappeared.class, this::executeLogRemoved)
                .match(Terminated.class, this::executeQuit)
                .matchAny(message -> {
                    _metrics.incrementCounter(UNKNOWN_COUNTER);
                    LOGGER.warn()
                            .setMessage("Unsupported message")
                            .addData("actor", self())
                            .addData("data", message)
                            .log();
                    unhandled(message);
                })
                .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        _instrument = context().system().scheduler().scheduleAtFixedRate(
                new FiniteDuration(0, TimeUnit.SECONDS), // Initial delay
                new FiniteDuration(500, TimeUnit.MILLISECONDS), // Interval
                getSelf(),
                "instrument",
                ExecutionContexts.global(),
                getSelf());
    }

    @Override
    public void postStop() throws Exception {
        _instrument.cancel();
        super.postStop();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("members", _members)
                .put("serviceMetrics", _serviceMetrics)
                .put("logs", _logs)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void executeLogRemoved(final LogFileDisappeared message) {
        _metrics.incrementCounter(LOG_REMOVED_COUNTER);
        if (_logs.contains(message.getFile())) {
            _logs.remove(message.getFile());
            broadcast(message);
        }
    }

    private void executeLogAdded(final LogFileAppeared message) {
        _metrics.incrementCounter(LOG_ADDED_COUNTER);
        if (!_logs.contains(message.getFile())) {
            _logs.add(message.getFile());
            notifyNewLog(message.getFile());
        }
    }

    private void executeLogsListRequest() {
        _metrics.incrementCounter(METRICS_LIST_COUNTER);
        getSender().tell(new LogsList(_logs), getSelf());
    }

    private void executeLogLine(final LogLine message) {
        _metrics.incrementCounter(LOG_LINE_COUNTER);
        registerLog(message.getFile());
        broadcast(message);
    }

    private void executeConnect(final Connect message) {
        _metrics.incrementCounter(CONNECT_COUNTER);

        // Add the connection to the pool to receive future metric reports
        _members.add(message.getConnection());
        context().watch(message.getConnection());

        LOGGER.info()
                .setMessage("Connection opened")
                .addData("actor", self())
                .addData("connection", message.getConnection())
                .log();
    }

    private void executePeriodicData(final PeriodicData message) {
        _metrics.incrementCounter(PERIODIC_DATA_COUNTER);

        // Ensure all the metrics are in the registry
        final Key dimensions = message.getDimensions();
        for (final Map.Entry<String, AggregatedData> entry : message.getData().entries()) {
            registerMetric(dimensions.getService(), entry.getKey(), entry.getValue().getStatistic().getName());
        }

        // Transmit the data to all members
        broadcast(message);
    }

    private void executeQuit(final Terminated message) {
        _metrics.incrementCounter(QUIT_COUNTER);

        // Remove the connection from the pool
        _members.remove(message.getActor());
    }

    private void executeMetricsListRequest() {
        _metrics.incrementCounter(METRICS_LIST_REQUEST);

        // Transmit a list of all registered metrics
        getSender().tell(new MetricsList(ImmutableMap.copyOf(_serviceMetrics)), getSelf());
    }

    private void registerLog(final Path logPath) {
        if (!_logs.contains(logPath)) {
            _logs.add(logPath);
            notifyNewLog(logPath);
        }
    }

    private void notifyNewLog(final Path logPath) {
        broadcast(new NewLog(logPath));
    }

    private void broadcast(final Object message) {
        for (final ActorRef ref : _members) {
            ref.tell(message, getSelf());
        }
    }

    private void registerMetric(final String service, final String metric, final String statistic) {
        if (!_serviceMetrics.containsKey(service)) {
            _serviceMetrics.put(service, Maps.newHashMap());
        }
        final Map<String, Set<String>> serviceMap = _serviceMetrics.get(service);

        if (!serviceMap.containsKey(metric)) {
            serviceMap.put(metric, Sets.newHashSet());
        }
        final Set<String> statistics = serviceMap.get(metric);

        if (!statistics.contains(statistic)) {
            statistics.add(statistic);
            notifyNewMetric(service, metric, statistic);
        }
    }

    private void notifyNewMetric(final String service, final String metric, final String statistic) {
        final NewMetric newMetric = new NewMetric(service, metric, statistic);
        broadcast(newMetric);
    }

    private void periodicInstrumentation() {
        _metrics.close();
        _metrics = _metricsFactory.create();
    }


    private Cancellable _instrument;
    private final Set<Path> _logs = Sets.newTreeSet();
    private final MetricsFactory _metricsFactory;
    private final Set<ActorRef> _members = Sets.newHashSet();
    private final Map<String, Map<String, Set<String>>> _serviceMetrics = Maps.newHashMap();

    private Metrics _metrics;

    private static final String METRIC_PREFIX = "actors/stream/";
    private static final String METRICS_LIST_REQUEST = METRIC_PREFIX + "metrics_list_request";
    private static final String QUIT_COUNTER = METRIC_PREFIX + "quit";
    private static final String PERIODIC_DATA_COUNTER = METRIC_PREFIX + "periodic_data";
    private static final String CONNECT_COUNTER = METRIC_PREFIX + "connect";
    private static final String LOG_LINE_COUNTER = METRIC_PREFIX + "log_line";
    private static final String METRICS_LIST_COUNTER = METRIC_PREFIX + "metrics_list";
    private static final String LOG_ADDED_COUNTER = METRIC_PREFIX + "log_added";
    private static final String LOG_REMOVED_COUNTER = METRIC_PREFIX + "log_removed";
    private static final String UNKNOWN_COUNTER = METRIC_PREFIX + "UNKNOWN";
    private static final Logger LOGGER = LoggerFactory.getLogger(Telemetry.class);
}
