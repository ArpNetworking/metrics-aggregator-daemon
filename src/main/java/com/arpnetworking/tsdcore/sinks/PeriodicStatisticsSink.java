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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.ValidateWithMethod;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
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
 * IMPORTANT: This sink counts total samples through it and will count samples
 * across periods. It is strongly recommended that you configure this sink on
 * a sink-chain for a single period (e.g. using PeriodFilteringSink). Further,
 * it assumes that all AggregatedData instances for a given metric contain a
 * correct PopulationSize and are thus interchangeable as far as sample counting
 * is concerned.
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
        final Key bucketKey = computeKey(periodicData.getDimensions(), _mappedDimensions);
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

    static Key computeKey(final Key key, final ImmutableMultimap<String, String> mappedDimensions) {
        // TODO(ville): Apply interning to creating Key instances

        // Filter the input key's parameters by the ones we wish to dimension map
        // and then create a new parameter set from the target (values) of the
        // dimension keys and the corresponding value from parameters for the
        // dimension.
        final Map<String, String> parameters = key.getParameters();
        return new DefaultKey(
                mappedDimensions.entries()
                        .stream()
                        .filter(e -> parameters.containsKey(e.getKey()))
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        Map.Entry::getValue,
                                        e -> parameters.get(e.getKey()))));
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

    private static <T> Set<T> createConcurrentSet(final Set<T> existingSet) {
        final int initialCapacity = (int) (existingSet.size() / 0.75);
        return Collections.newSetFromMap(new ConcurrentHashMap<>(initialCapacity));
    }

    // NOTE: Package private for testing
    /* package private */ PeriodicStatisticsSink(final Builder builder, final ScheduledExecutorService executor) {
        super(builder);

        // Merge the mapped and unmapped dimensions
        final ImmutableMultimap.Builder<String, String> dimensionsBuilder = ImmutableMultimap.builder();
        dimensionsBuilder.putAll(
                builder._dimensions.stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e,
                                        e -> e))
                        .entrySet());
        dimensionsBuilder.putAll(builder._mappedDimensions.entrySet());

        // Initialize the metrics factory and metrics instance
        _mappedDimensions = dimensionsBuilder.build();
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

    private final ImmutableMultimap<String, String> _mappedDimensions;
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
    private static final Key GLOBAL_KEY = new DefaultKey(ImmutableMap.of());

    private final class MetricsLogger implements Runnable {

        @Override
        public void run() {
            flushMetrics();
        }
    }

    private static class MetricKey {

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof MetricKey)) {
                return false;
            }

            final MetricKey otherKey = (MetricKey) other;
            return Objects.equals(_metricName, otherKey._metricName)
                    && Objects.equals(_key, otherKey._key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(_metricName, _key);
        }

        MetricKey(final String metricName, final Key key) {
            _metricName = metricName;
            _key = key;
        }

        private final String _metricName;
        private final Key _key;
    }

    private class Bucket {

        public void record(final PeriodicData periodicData, final long now) {
            final ImmutableMultimap<String, AggregatedData> data = periodicData.getData();
            _aggregatedData.addAndGet(data.size());

            for (final String metricName : data.keySet()) {
                final AggregatedData firstDatum = data.get(metricName).iterator().next();

                // TODO(ville): These need to be converted to use set cardinality.
                // As-is they are not at all useful across hosts or when tagged.
                final MetricKey metricKey = new MetricKey(metricName, periodicData.getDimensions());
                _uniqueMetrics.get().add(metricName);
                _uniqueStatistics.get().add(metricKey);

                // This assumes that all AggregatedData instances have the
                // population field set correctly (really they should).
                _metricSamples.accumulate(firstDatum.getPopulationSize());
            }

            _age.accumulate(now - periodicData.getStart().plus(periodicData.getPeriod()).toInstant().toEpochMilli());
        }

        public boolean flushMetrics() {
            // Rotate the metrics instance
            final Metrics metrics = _metrics.getAndSet(createMetrics());

            // Gather and reset state
            final Set<String> oldUniqueMetrics = _uniqueMetrics.getAndSet(
                    createConcurrentSet(_uniqueMetrics.get()));
            final Set<MetricKey> oldUniqueStatistics = _uniqueStatistics.getAndSet(
                    createConcurrentSet(_uniqueStatistics.get()));

            // Record statistics and close
            metrics.incrementCounter(_aggregatedDataName, _aggregatedData.getAndSet(0));
            metrics.incrementCounter(_uniqueMetricsName, oldUniqueMetrics.size());
            metrics.incrementCounter(_uniqueStatisticsName, oldUniqueStatistics.size());
            metrics.incrementCounter(_metricSamplesName, _metricSamples.getThenReset());
            metrics.setTimer(_ageName, _age.getThenReset(), TimeUnit.MILLISECONDS);
            metrics.close();

            // Use unique metrics as a proxy for whether data was added. See note in the sink's
            // flushMetrics method about how the race condition is resolved.
            return !oldUniqueMetrics.isEmpty();
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
        private final AtomicReference<Set<MetricKey>> _uniqueStatistics = new AtomicReference<>(
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
         * null. Default is empty set (no dimension partitions).
         *
         * Note that if the same dimension is specified here and in mapped
         * dimensions the output key from mapped dimensions takes precedence.
         *
         * @param value The set of dimension names to partition on.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setDimensions(final ImmutableSet<String> value) {
            _dimensions = value;
            return this;
        }

        /**
         * Incoming dimension names to partition the periodic statistics on
         * mapped to an outbound dimension name. Cannot be null. Default is
         * empty map (no dimension partitions).
         *
         * Note that if the same dimension is specified here and in
         * dimensions the output key from mapped dimensions takes precedence.
         *
         * @param value The set of dimension names to partition on.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMappedDimensions(final ImmutableMap<String, String> value) {
            _mappedDimensions = value;
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

        private boolean validate(final ImmutableMap<String, String> ignored) {
            final Set<String> mappedTargetDimensions = Sets.newHashSet(_mappedDimensions.values());

            // Assert that no two mapped dimensions target the same value
            if (mappedTargetDimensions.size() != _mappedDimensions.size()) {
                return false;
            }

            // Assert that no mapped dimension target is also a dimension
            if (!Sets.intersection(mappedTargetDimensions, _dimensions).isEmpty()) {
                return false;
            }

            return true;
        }

        @NotNull
        @Min(value = 1)
        private Long _intervalInMilliseconds = 500L;
        @NotNull
        private ImmutableSet<String> _dimensions = ImmutableSet.of();
        @NotNull
        @ValidateWithMethod(methodName = "validate", parameterType = ImmutableMap.class)
        private ImmutableMap<String, String> _mappedDimensions = ImmutableMap.of();
        @JacksonInject
        @NotNull
        private MetricsFactory _metricsFactory;
    }
}
