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
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
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
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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
 * record; effectively, this is metrics about metrics.
 *
 * The sink's original and primary purpose is to provide a quick sanity check
 * on installations by generating metrics that the aggregator can then consume
 * (and use to generate more metrics).
 *
 * More recently, this class has been extended to allow attribution of usage
 * by bucketing and tagging the emitted periodic self-instrumentation metrics
 * using configured tag keys who's values are lighted from the data flow itself.
 *
 * This class is thread safe.
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

        // Record the periodic data against the appropriate bucket
        // NOTE: if no dimensions are configured then the bucket key is an empty map
        final Key bucketKey = periodicData.getDimensions().filter(_dimensions);
        _buckets.computeIfAbsent(bucketKey, k -> new Bucket(bucketKey))
                .record(periodicData, now);
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
        flushMetrics();
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .build();
    }

    private void flushMetrics() {
        // This two phase approach to removing unused buckets aims to avoid a race
        // condition between the flush+remove and any new data added.
        //
        // 1) If the bucket flush does nothing then remove it from the buckets map and
        // add it to a "to be removed" list. This prevents further
        //
        // 2) On the next flush interval re-flush all the buckets to be removed and then
        // actually drop them.
        //
        // In the meantime any new data added to the key for the bucket to be removed will
        // discover that it was removed from the buckets map and recreate it.
        //
        // This assumes that the flush interval is longer than the time to record metrics
        // against a bucket instance. To help guarantee this the minimum flush interval is
        // already set at 1ms.

        // Re-Flush any buckets to be removed and remove them
        _bucketsToBeRemoved.forEach(Bucket::flushMetrics);
        _bucketsToBeRemoved.clear();

        // Flush the current buckets
        final Iterator<Map.Entry<Key, Bucket>> iterator = _buckets.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Key, Bucket> entry = iterator.next();
            final Bucket bucket = entry.getValue();
            final Key key = entry.getKey();

            // Remove the bucket from the map and add it for removal if:
            // 1) The bucket does not flush any data
            // 2) The bucket's key is not the global key
            //
            // Not removing the global key ensures that we always produce a
            // periodic series specifically to indicate that no data is being
            // processed (as opposed to inferring that from the absence of data).
            if (!bucket.flushMetrics() && !GLOBAL_KEY.equals(key)) {
                _bucketsToBeRemoved.add(bucket);
                iterator.remove();
            }
        }
    }

    private static Set<String> createConcurrentSet(final Set<String> existingSet) {
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
        _dimensions = builder._dimensions;
        _metricsFactory = builder._metricsFactory;
        _buckets = Maps.newConcurrentMap();

        _aggregatedDataName = "sinks/periodic_statistics/" + getMetricSafeName() + "/aggregated_data";
        _uniqueMetricsName = "sinks/periodic_statistics/" + getMetricSafeName() + "/unique_metrics";
        _uniqueStatisticsName = "sinks/periodic_statistics/" + getMetricSafeName() + "/unique_statistics";
        _metricSamplesName = "sinks/periodic_statistics/" + getMetricSafeName() + "/metric_samples";
        _ageName = "sinks/periodic_statistics/" + getMetricSafeName() + "/age";

        // Create the global bucket
        // NOTE: This means the sink publishes zeros even if no data is flowing
        _buckets.put(GLOBAL_KEY, new Bucket(GLOBAL_KEY));

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

    private final ImmutableSet<String> _dimensions;
    private final Map<Key, Bucket> _buckets;
    private final MetricsFactory _metricsFactory;
    private final Deque<Bucket> _bucketsToBeRemoved = new ConcurrentLinkedDeque<>();

    private final String _aggregatedDataName;
    private final String _uniqueMetricsName;
    private final String _uniqueStatisticsName;
    private final String _metricSamplesName;
    private final String _ageName;

    private final ScheduledExecutorService _executor;

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicStatisticsSink.class);
    private static final int EXECUTOR_TIMEOUT_IN_SECONDS = 30;
    private static final ImmutableMap<Duration, String> CACHED_PERIOD_STRINGS;
    private static final Key GLOBAL_KEY = new DefaultKey(ImmutableMap.of());

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
            flushMetrics();
        }
    }

    private class Bucket {

        public void record(final PeriodicData periodicData, final long now) {
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

        public boolean flushMetrics() {
            // Rotate the metrics instance
            final Metrics metrics = _metrics.getAndSet(createMetrics());

            // Gather and reset state
            final Set<String> oldUniqueMetrics = _uniqueMetrics.getAndSet(
                    createConcurrentSet(_uniqueMetrics.get()));
            final Set<String> oldUniqueStatistics = _uniqueStatistics.getAndSet(
                    createConcurrentSet(_uniqueStatistics.get()));

            // Record statistics and close
            metrics.incrementCounter(_aggregatedDataName, _aggregatedData.getAndSet(0));
            metrics.incrementCounter(_uniqueMetricsName, oldUniqueMetrics.size());
            metrics.incrementCounter(_uniqueStatisticsName, oldUniqueStatistics.size());
            metrics.incrementCounter(_metricSamplesName, _metricSamples.get());

            // Use age as a proxy for whether data was added. See note in the sink's
            // flushMetrics method about how the race condition is resolved.
            final long age = _age.getThenReset();
            metrics.setTimer(_ageName, age, TimeUnit.MILLISECONDS);
            metrics.close();

            return age > 0;
        }

        @LogValue
        public Object toLogValue() {
            return LogValueMapFactory.builder(this)
                    .put("aggregatedData", _aggregatedData)
                    .put("uniqueMetrics", _uniqueMetrics.get().size())
                    .put("uniqueStatistics", _uniqueStatistics.get().size())
                    .put("metricSamples", _metricSamples.get())
                    .build();
        }

        private Metrics createMetrics() {
            final Metrics metrics = _metricsFactory.create();
            metrics.addAnnotations(_key.getParameters());
            metrics.resetCounter(_aggregatedDataName);
            metrics.resetCounter(_uniqueMetricsName);
            metrics.resetCounter(_uniqueStatisticsName);
            metrics.resetCounter(_metricSamplesName);
            return metrics;
        }

        Bucket(final Key key) {
            _key = key;
            _metrics.set(createMetrics());
        }

        private final Key _key;
        private final AtomicReference<Metrics> _metrics = new AtomicReference<>();

        private final LongAccumulator _age = new LongAccumulator(Math::max, 0);
        private final LongAccumulator _metricSamples = new LongAccumulator(Long::sum, 0);
        private final AtomicLong _aggregatedData = new AtomicLong(0);
        private final AtomicReference<Set<String>> _uniqueMetrics = new AtomicReference<>(
                Collections.newSetFromMap(Maps.newConcurrentMap()));
        private final AtomicReference<Set<String>> _uniqueStatistics = new AtomicReference<>(
                Collections.newSetFromMap(Maps.newConcurrentMap()));
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
         * Dimension names to partition the periodic statistics on. Cannot be
         * null. Default is empty set (no dimennsion partitions).
         *
         * @param value The set of dimension names to partition on.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setDimensions(final ImmutableSet<String> value) {
            _dimensions = value;
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
        @NotNull
        private ImmutableSet<String> _dimensions = ImmutableSet.of();
        @JacksonInject
        @NotNull
        private MetricsFactory _metricsFactory;
    }
}
