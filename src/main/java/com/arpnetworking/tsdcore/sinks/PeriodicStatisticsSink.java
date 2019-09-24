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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Aggregates and periodically logs metrics about the aggregated data being
 * record; effectively, this is metrics about metrics. It's primary purpose is
 * to provide a quick sanity check on installations by generating metrics that
 * the aggregator can then consume (and use to generate more metrics). This
 * class is thread safe.
 *
 * TODO(vkoskela): Remove synchronized blocks [MAI-110]
 *
 * Details: The synchronization can be removed if the metrics client can
 * be configured to throw ISE when attempting to write to a closed instance.
 * This would allow a retry on the new instance; starvation would theoretically
 * be possible but practically should never happen.
 *
 * (+) The implementation of _age as an AtomicLong currently relies on the
 * locking provided by the synchronized block to perform it's check and set.
 * This can be replaced with a separate lock or a thread-safe accumulator
 * implementation.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class PeriodicStatisticsSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", periodicData.getData().size())
                .log();

        final long now = System.currentTimeMillis();
        _aggregatedData.addAndGet(periodicData.getData().size());

        final String fqsnPrefix = new StringBuilder()
                .append(periodicData.getDimensions().getParameters().entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining(";")))
                .append(getPeriodAsString(periodicData.getPeriod()))
                .toString();
        final String serviceMetricPrefix = new StringBuilder()
                .append(periodicData.getDimensions().getService()).append(".")
                .toString();

        final Set<String> uniqueMetrics = Sets.newHashSet();
        for (final Map.Entry<String, AggregatedData> entry : periodicData.getData().entries()) {
            final String metricName = entry.getKey();
            final AggregatedData datum = entry.getValue();
            final String fqsn = new StringBuilder()
                    .append(fqsnPrefix)
                    .append(metricName).append(".")
                    .append(datum.getStatistic().getName()).append(".")
                    .toString();

            final String serviceMetric = new StringBuilder()
                    .append(serviceMetricPrefix)
                    .append(metricName)
                    .toString();

            _uniqueMetrics.get().add(serviceMetric);

            _uniqueStatistics.get().add(fqsn);

            if (uniqueMetrics.add(serviceMetric)) {
                // Allow each service/metric in the periodic data to contribute
                // its population size (samples processed) to the sample count.
                _metricSamples.accumulate(datum.getPopulationSize());
            }
        }

        _age.accumulate(now - periodicData.getStart().plus(periodicData.getPeriod()).toInstant().toEpochMilli());
    }

    @Override
    public void close() {
        try {
            _executor.shutdown();
            _executor.awaitTermination(EXECUTOR_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException(e);
        }
        flushMetrics(_metrics.get());
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("aggregatedData", _aggregatedData)
                .put("uniqueMetrics", _uniqueMetrics.get().size())
                .put("uniqueStatistics", _uniqueStatistics.get().size())
                .put("metricSamples", _metricSamples.get())
                .build();
    }

    private void flushMetrics(final Metrics metrics) {
        // Gather and reset state
        final Set<String> oldUniqueMetrics = _uniqueMetrics.getAndSet(
                createConcurrentSet(_uniqueMetrics.get()));
        final Set<String> oldUniqueStatistics = _uniqueStatistics.getAndSet(
                createConcurrentSet(_uniqueStatistics.get()));

        // Record statistics and close
        metrics.incrementCounter(_aggregatedDataName, _aggregatedData.getAndSet(0));
        metrics.incrementCounter(_uniqueMetricsName, oldUniqueMetrics.size());
        metrics.incrementCounter(_uniqueStatisticsName, oldUniqueStatistics.size());
        metrics.incrementCounter(_metricSamplesName, _metricSamples.getThenReset());
        metrics.setTimer(_ageName, _age.getThenReset(), TimeUnit.MILLISECONDS);
        metrics.close();
    }

    private Metrics createMetrics() {
        final Metrics metrics = _metricsFactory.create();
        metrics.resetCounter(_aggregatedDataName);
        metrics.resetCounter(_uniqueMetricsName);
        metrics.resetCounter(_uniqueStatisticsName);
        metrics.resetCounter(_metricSamplesName);
        return metrics;
    }

    private Set<String> createConcurrentSet(final Set<String> existingSet) {
        final int initialCapacity = (int) (existingSet.size() / 0.75);
        return Collections.newSetFromMap(new ConcurrentHashMap<>(initialCapacity));
    }

    private static String getPeriodAsString(final Duration period) {
        // TODO(ville): This is the only use of period serialization in MAD (weird, eh?)
        // However, we should consider generalizing and moving this to commons.

        @Nullable final String periodAsString = CACHED_PERIOD_STRINGS.get(period);
        if (periodAsString == null) {
            return period.toString();
        }
        return periodAsString;
    }

    // NOTE: Package private for testing
    /* package private */ PeriodicStatisticsSink(final Builder builder, final ScheduledExecutorService executor) {
        super(builder);

        // Initialize the metrics factory and metrics instance
        _metricsFactory = builder._metricsFactory;
        _aggregatedDataName = "sinks/periodic_statistics/" + getMetricSafeName() + "/aggregated_data";
        _uniqueMetricsName = "sinks/periodic_statistics/" + getMetricSafeName() + "/unique_metrics";
        _uniqueStatisticsName = "sinks/periodic_statistics/" + getMetricSafeName() + "/unique_statistics";
        _metricSamplesName = "sinks/periodic_statistics/" + getMetricSafeName() + "/metric_samples";
        _ageName = "sinks/periodic_statistics/" + getMetricSafeName() + "/age";
        _metrics.set(createMetrics());

        // Write the metrics periodically
        _executor = executor;
        _executor.scheduleAtFixedRate(
                new MetricsLogger(),
                builder._intervalInMilliseconds,
                builder._intervalInMilliseconds,
                TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("unused") // Invoked reflectively from Builder
    private PeriodicStatisticsSink(final Builder builder) {
        this(builder, Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "PeriodStatisticsSink")));
    }

    private final MetricsFactory _metricsFactory;
    private final AtomicReference<Metrics> _metrics = new AtomicReference<>();

    private final LongAccumulator _age = new LongAccumulator(Math::max, 0);
    private final String _aggregatedDataName;
    private final String _uniqueMetricsName;
    private final String _uniqueStatisticsName;
    private final String _metricSamplesName;
    private final String _ageName;
    private final LongAccumulator _metricSamples = new LongAccumulator((x, y) -> x + y, 0);
    private final AtomicLong _aggregatedData = new AtomicLong(0);
    private final AtomicReference<Set<String>> _uniqueMetrics = new AtomicReference<>(
            Collections.newSetFromMap(Maps.<String, Boolean>newConcurrentMap()));
    private final AtomicReference<Set<String>> _uniqueStatistics = new AtomicReference<>(
            Collections.newSetFromMap(Maps.<String, Boolean>newConcurrentMap()));

    private final ScheduledExecutorService _executor;

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicStatisticsSink.class);
    private static final int EXECUTOR_TIMEOUT_IN_SECONDS = 30;
    private static final ImmutableMap<Duration, String> CACHED_PERIOD_STRINGS;

    static {
        final ImmutableSet<Duration> periods = ImmutableSet.<Duration>builder()
                .add(Duration.ofSeconds(1))
                .add(Duration.ofMinutes(1))
                .add(Duration.ofMinutes(2))
                .add(Duration.ofMinutes(5))
                .add(Duration.ofMinutes(10))
                .add(Duration.ofMinutes(15))
                .add(Duration.ofHours(1))
                .add(Duration.ofDays(1))
                .build();
        final ImmutableMap.Builder<Duration, String> builder = ImmutableMap.builder();
        for (final Duration period : periods) {
            builder.put(period, period.toString());
        }
        CACHED_PERIOD_STRINGS = builder.build();
    }

    private final class MetricsLogger implements Runnable {

        @Override
        public void run() {
            final Metrics oldMetrics = _metrics.getAndSet(createMetrics());
            flushMetrics(oldMetrics);
        }
    }

    /**
     * Implementation of builder pattern for <code>PeriodicStatisticsSink</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends BaseSink.Builder<Builder, PeriodicStatisticsSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(PeriodicStatisticsSink::new);
        }

        /**
         * The interval in milliseconds between statistic flushes. Cannot be null;
         * minimum 1. Default is 1.
         *
         * @param value The interval in seconds between flushes.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setIntervalInMilliseconds(final Long value) {
            _intervalInMilliseconds = value;
            return this;
        }

        /**
         * Instance of <code>MetricsFactory</code>. Cannot be null. This field
         * may be injected automatically by Jackson/Guice if setup to do so.
         *
         * @param value Instance of <code>MetricsFactory</code>.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMetricsFactory(final MetricsFactory value) {
            _metricsFactory = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        @Min(value = 1)
        private Long _intervalInMilliseconds = 500L;
        @JacksonInject
        @NotNull
        private MetricsFactory _metricsFactory;
    }
}
