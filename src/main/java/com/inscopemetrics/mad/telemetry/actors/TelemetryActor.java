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
package com.inscopemetrics.mad.telemetry.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.dispatch.ExecutionContexts;
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.inscopemetrics.mad.model.AggregatedData;
import com.inscopemetrics.mad.model.Key;
import com.inscopemetrics.mad.model.PeriodicData;
import com.inscopemetrics.mad.model.Quantity;
import com.inscopemetrics.mad.statistics.HistogramStatistic;
import com.inscopemetrics.mad.statistics.Statistic;
import com.inscopemetrics.mad.statistics.StatisticFactory;
import com.inscopemetrics.mad.statistics.TPStatistic;
import com.inscopemetrics.mad.telemetry.models.messages.Connect;
import com.inscopemetrics.mad.telemetry.models.messages.MetricsList;
import com.inscopemetrics.mad.telemetry.models.messages.MetricsListRequest;
import com.inscopemetrics.mad.telemetry.models.messages.NewMetric;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Actor responsible for holding the set of connected websockets and publishing
 * metrics to them.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Mohammed Kamel (mkamel at groupon dot com)
 */
public class TelemetryActor extends AbstractActor {

    /**
     * Factory for creating a {@code Props} with strong typing.
     *
     * @param metricsFactory instance of {@code MetricsFactory}
     * @param histogramStatistics statistics to send in lieu of a histogram
     * @return a new {@code Props} object to create a {@link TelemetryActor}
     */
    public static Props props(
            final MetricsFactory metricsFactory,
            final ImmutableSet<Statistic> histogramStatistics) {
        return Props.create(
                () -> new TelemetryActor(
                        metricsFactory,
                        histogramStatistics));
    }

    /**
     * Public constructor.
     *
     * @param metricsFactory instance of {@code MetricsFactory}.
     * @param histogramStatistics statistics to send in lieu of a histogram
     */
    @Inject
    public TelemetryActor(
            final MetricsFactory metricsFactory,
            final ImmutableSet<Statistic> histogramStatistics) {
        _metricsFactory = metricsFactory;
        _histogramStatistics = histogramStatistics;
        _metrics = metricsFactory.create();
        _instrument = context().system().scheduler().schedule(
                new FiniteDuration(0, TimeUnit.SECONDS), // Initial delay
                new FiniteDuration(500, TimeUnit.MILLISECONDS), // Interval
                getSelf(),
                "instrument",
                ExecutionContexts.global(),
                getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("instrument", message -> periodicInstrumentation())
                .match(PeriodicData.class, this::executePeriodicData)
                .match(Connect.class, this::executeConnect)
                .match(MetricsListRequest.class, ignored -> executeMetricsListRequest())
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

    private void executeConnect(final Connect message) {
        _metrics.incrementCounter(CONNECT_COUNTER);

        // Add the connection to the pool to receive future metric reports
        _members.add(message.getConnection());
        context().watch(message.getConnection());

        LOGGER.info()
                .setMessage("ConnectionActor opened")
                .addData("actor", self())
                .addData("connection", message.getConnection())
                .log();
    }

    private void executePeriodicData(final PeriodicData periodicData) {
        _metrics.incrementCounter(PERIODIC_DATA_COUNTER);

        // Ensure all the metrics are in the registry and replace histogram statistics
        final PeriodicData.Builder modifiedPeriodicDataBuilder = ThreadLocalBuilder.clone(periodicData);
        final ImmutableMultimap.Builder<String, AggregatedData> modifiedMetricsBuilder = ImmutableMultimap.builder();
        final Key dimensions = periodicData.getDimensions();

        for (final Map.Entry<String, AggregatedData> entry : periodicData.getData().entries()) {
            if (!HISTOGRAM_STATISTIC.equals(entry.getValue().getStatistic())) {
                registerMetric(dimensions.getService(), entry.getKey(), entry.getValue().getStatistic().getName());
                modifiedMetricsBuilder.put(entry.getKey(), entry.getValue());
            } else {
                for (final Statistic statistic : _histogramStatistics) {
                    registerMetric(dimensions.getService(), entry.getKey(), statistic.getName());

                    final Optional<AggregatedData> percentileAggregatedData = computePercentile(
                            entry.getValue(),
                            statistic);
                    if (percentileAggregatedData.isPresent()) {
                        modifiedMetricsBuilder.put(entry.getKey(), percentileAggregatedData.get());
                    }
                }
            }
        }
        modifiedPeriodicDataBuilder.setData(modifiedMetricsBuilder.build());

        // Transmit the data to all members
        broadcast(modifiedPeriodicDataBuilder.build());
    }

    private Optional<AggregatedData> computePercentile(final AggregatedData value, final Statistic statistic) {
        if (value.getSupportingData() instanceof HistogramStatistic.HistogramSupportingData
                && statistic instanceof TPStatistic) {
            final TPStatistic tpStatistic = (TPStatistic) statistic;

            final HistogramStatistic.HistogramSupportingData supportingData =
                    (HistogramStatistic.HistogramSupportingData) value.getSupportingData();

            final double percentile = supportingData.getHistogramSnapshot().getValueAtPercentile(
                    tpStatistic.getPercentile());

            return Optional.of(
                    new AggregatedData.Builder()
                            .setStatistic(statistic)
                            .setIsSpecified(true)
                            .setValue(ThreadLocalBuilder.build(
                                    Quantity.Builder.class,
                                    b -> b.setValue(percentile)
                                            .setUnit(supportingData.getUnit().orElse(null))))
                            .setPopulationSize(value.getPopulationSize())
                            .build());
        }
        return Optional.empty();
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


    private final Cancellable _instrument;
    private final Set<Path> _logs = Sets.newTreeSet();
    private final MetricsFactory _metricsFactory;
    private final ImmutableSet<Statistic> _histogramStatistics;
    private final Set<ActorRef> _members = Sets.newHashSet();
    private final Map<String, Map<String, Set<String>>> _serviceMetrics = Maps.newHashMap();

    private Metrics _metrics;

    private static final String METRIC_PREFIX = "actors/stream/";
    private static final String METRICS_LIST_REQUEST = METRIC_PREFIX + "metrics_list_request";
    private static final String QUIT_COUNTER = METRIC_PREFIX + "quit";
    private static final String PERIODIC_DATA_COUNTER = METRIC_PREFIX + "periodic_data";
    private static final String CONNECT_COUNTER = METRIC_PREFIX + "connect";
    private static final String UNKNOWN_COUNTER = METRIC_PREFIX + "UNKNOWN";
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic HISTOGRAM_STATISTIC = STATISTIC_FACTORY.getStatistic("histogram");
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryActor.class);
}
