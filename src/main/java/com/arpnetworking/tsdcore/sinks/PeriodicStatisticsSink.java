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
import net.sf.oval.constraint.CheckWith;
import net.sf.oval.constraint.CheckWithCheck;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
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
        final Key bucketKey = computeKey(
                periodicData.getDimensions(),
                _periodDimensionName,
                periodicData.getPeriod(),
                _mappedDimensions,
                _defaultDimensionValues);
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

    static Key computeKey(
            final Key key,
            final String periodDimensionName,
            final Duration period,
            final ImmutableMultimap<String, String> mappedDimensions,
            final ImmutableMap<String, String> defaultDimensionValues) {
        // TODO(ville): Apply interning to creating Key instances

        // Filter the input key's parameters by the ones we wish to dimension map
        // and then create a new parameter set from the target (values) of the
        // dimension keys and the corresponding value from parameters for the
        // dimension.
        //
        // There are no key collisions possible because the inputs that created
        // mappedDimensions were validated as only declaring each target key
        // name once (regardless of how it was populated).
        final Map<String, String> parameters = key.getParameters();

        final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
        dimensionsBuilder.put(periodDimensionName, getPeriodAsString(period));

        mappedDimensions.entries()
                .stream()
                .filter(e -> parameters.containsKey(e.getKey()) || defaultDimensionValues.containsKey(e.getKey()))
                .forEach(e -> dimensionsBuilder.put(
                        e.getValue(),
                        parameters.getOrDefault(e.getKey(), defaultDimensionValues.get(e.getKey()))));

        return new DefaultKey(dimensionsBuilder.build());
    }

    private void flushMetrics() {
        // This two phase approach to removing unused buckets aims to avoid a race
        // condition between the flush+remove and any new data added.
        //
        // 1) If the bucket flush does nothing then remove it from the buckets map and
        // add it to a "to be removed" list. This prevents buckets from accumulating
        // in memory if the tag space changes over time.
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

            // Remove the bucket from the map and add it for removal if the
            // bucket does not flush any data.
            if (!bucket.flushMetrics()) {
                _bucketsToBeRemoved.add(bucket);
                iterator.remove();
            }
        }
    }

    ImmutableMultimap<String, String> getMappedDimensions() {
        return _mappedDimensions;
    }

    private static <T> Set<T> createConcurrentSet(final Set<T> existingSet) {
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

        // Merge the mapped and unmapped dimensions
        //
        // The validation in the Builder ensures that all target key names
        // occur only once across dimensions and mappedDimensions regardless
        // of what the source key name is (or whether it was unmapped or mapped).
        final ImmutableMultimap.Builder<String, String> dimensionsBuilder = ImmutableMultimap.builder();
        builder._dimensions.forEach(e -> dimensionsBuilder.put(e, e));
        dimensionsBuilder.putAll(builder._mappedDimensions.entrySet());

        // Initialize the metrics factory and metrics instance
        _mappedDimensions = dimensionsBuilder.build();
        _defaultDimensionValues = builder._defaultDimensionValues;
        _metricsFactory = builder._metricsFactory;
        _buckets = Maps.newConcurrentMap();
        _periodDimensionName = builder._periodDimensionName;

        _aggregatedDataName = "sinks/periodic_statistics/" + getMetricSafeName() + "/aggregated_data";
        _uniqueMetricsName = "sinks/periodic_statistics/" + getMetricSafeName() + "/unique_metrics";
        _uniqueStatisticsName = "sinks/periodic_statistics/" + getMetricSafeName() + "/unique_statistics";
        _metricSamplesName = "sinks/periodic_statistics/" + getMetricSafeName() + "/metric_samples";
        _ageName = "sinks/periodic_statistics/" + getMetricSafeName() + "/age";
        _maxRequestAgeName = "sinks/periodic_statistics/" + getMetricSafeName() + "/request_age";

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
    private final ImmutableMap<String, String> _defaultDimensionValues;
    private final Map<Key, Bucket> _buckets;
    private final MetricsFactory _metricsFactory;
    private final Deque<Bucket> _bucketsToBeRemoved = new ConcurrentLinkedDeque<>();

    private final String _periodDimensionName;
    private final String _aggregatedDataName;
    private final String _uniqueMetricsName;
    private final String _uniqueStatisticsName;
    private final String _metricSamplesName;
    private final String _ageName;
    private final String _maxRequestAgeName;

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
                _metricSamples.addAndGet(firstDatum.getPopulationSize());
            }

            _age.accumulate(now - periodicData.getStart().plus(periodicData.getPeriod()).toInstant().toEpochMilli());
            periodicData.getMinRequestTime().ifPresent(
                    t -> _maxRequestAge.accumulate(now - t.toInstant().toEpochMilli()));
        }

        public boolean flushMetrics() {
            // Gather and reset state
            final Set<String> oldUniqueMetrics = _uniqueMetrics.getAndSet(
                    createConcurrentSet(_uniqueMetrics.get()));
            final Set<MetricKey> oldUniqueStatistics = _uniqueStatistics.getAndSet(
                    createConcurrentSet(_uniqueStatistics.get()));

            // Use unique metrics as a proxy for whether data was added. See note in the sink's
            // flushMetrics method about how the race condition is resolved.
            if (!oldUniqueMetrics.isEmpty()) {
                // Rotate the metrics instance
                final Metrics metrics = _metrics.getAndSet(createMetrics());

                // Record statistics and close
                metrics.incrementCounter(_aggregatedDataName, _aggregatedData.getAndSet(0));
                metrics.incrementCounter(_uniqueMetricsName, oldUniqueMetrics.size());
                metrics.incrementCounter(_uniqueStatisticsName, oldUniqueStatistics.size());
                metrics.incrementCounter(_metricSamplesName, _metricSamples.getAndSet(0));
                metrics.setTimer(_ageName, _age.getThenReset(), TimeUnit.MILLISECONDS);
                metrics.setTimer(_maxRequestAgeName, _maxRequestAge.getThenReset(), TimeUnit.MILLISECONDS);
                metrics.close();

                // Periodic data was flushed
                return true;
            }

            // No periodic data was flushed; this bucket can be cleaned up
            return false;
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
            return metrics;
        }

        Bucket(final Key key) {
            _key = key;
            _metrics.set(createMetrics());
        }

        private final Key _key;
        private final AtomicReference<Metrics> _metrics = new AtomicReference<>();

        private final LongAccumulator _age = new LongAccumulator(Math::max, 0);
        private final LongAccumulator _maxRequestAge = new LongAccumulator(Math::max, 0);
        private final AtomicLong _metricSamples = new AtomicLong(0);
        private final AtomicLong _aggregatedData = new AtomicLong(0);
        private final AtomicReference<Set<String>> _uniqueMetrics = new AtomicReference<>(
                ConcurrentHashMap.newKeySet());
        private final AtomicReference<Set<MetricKey>> _uniqueStatistics = new AtomicReference<>(
                ConcurrentHashMap.newKeySet());
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
         * These dimensions create a target dimension on the periodic data of
         * the same name. These output dimension names are not allowed to
         * overlap between those specified (implicitly) via
         * {@link Builder#setDimensions(ImmutableSet)} and (explicitly)
         * via {@link Builder#setMappedDimensions(ImmutableMap)}.
         * Doing so will cause a validation failure.
         *
         * Further, the output dimension names must be distinct from the period
         * dimension name set by {@link Builder#setPeriodDimensionName(String)}
         * and which defaults to "_period".
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
         * These dimensions create a target dimension on the periodic data of
         * the specified name. These output dimension names are not allowed to
         * overlap between those specified (implicitly) via
         * {@link Builder#setDimensions(ImmutableSet)} and (explicitly)
         * via {@link Builder#setMappedDimensions(ImmutableMap)}.
         * Doing so will cause a validation failure.
         *
         * Further, the output dimension names must be distinct from the period
         * dimension name set by {@link Builder#setPeriodDimensionName(String)}
         * and which defaults to "_period".
         *
         * @param value The set of dimension names to partition on.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMappedDimensions(final ImmutableMap<String, String> value) {
            _mappedDimensions = value;
            return this;
        }

        /**
         * Supply default dimension values by original dimension key. The keys
         * in this map must also be specified either in {@link Builder#setDimensions(ImmutableSet)}
         * or as keys in {@link Builder#setMappedDimensions(ImmutableMap)}. The
         * key refers to the dimension name on the input {@link PeriodicData}.
         *
         * @param value The default dimension key-value pairs.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setDefaultDimensionsValues(final ImmutableMap<String, String> value) {
            _defaultDimensionValues = value;
            return this;
        }

        /**
         * The name of the outbound dimension for the periodicity of the data.
         * Cannot be null. Default is "_period".
         *
         * @param value The name of the outbound dimension for periodicity.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setPeriodDimensionName(final String value) {
            _periodDimensionName = value;
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
        @CheckWith(value = CheckUniqueDimensionTargets.class)
        private ImmutableSet<String> _dimensions = ImmutableSet.of();
        @NotNull
        private ImmutableMap<String, String> _mappedDimensions = ImmutableMap.of();
        @NotNull
        @CheckWith(value = CheckDefaultDimensionTargets.class)
        private ImmutableMap<String, String> _defaultDimensionValues = ImmutableMap.of();
        @NotNull
        private String _periodDimensionName = "_period";
        @JacksonInject
        @NotNull
        private MetricsFactory _metricsFactory;

        private static final class CheckUniqueDimensionTargets implements CheckWithCheck.SimpleCheck {

            private static final long serialVersionUID = -1484528750004342337L;

            @Override
            public boolean isSatisfied(final Object validatedObject, final Object value) {
                // TODO(ville): Find a way to throw validation exceptions instead of logging.

                if (!(validatedObject instanceof PeriodicStatisticsSink.Builder)) {
                    return false;
                }
                final PeriodicStatisticsSink.Builder builder = (PeriodicStatisticsSink.Builder) validatedObject;

                final Set<String> mappedTargetDimensions = Sets.newHashSet(builder._mappedDimensions.values());

                // Assert that no two mapped dimensions target the same value
                if (mappedTargetDimensions.size() != builder._mappedDimensions.size()) {
                    LOGGER.warn()
                            .setMessage("Invalid PeriodicStatisticsSink")
                            .addData("reason", "Mapped dimensions target the same key name")
                            .addData("mappedDimensions", builder._mappedDimensions)
                            .log();
                    return false;
                }

                // Assert that no mapped dimension target is also a dimension
                if (!Sets.intersection(mappedTargetDimensions, builder._dimensions).isEmpty()) {
                    LOGGER.warn()
                            .setMessage("Invalid PeriodicStatisticsSink")
                            .addData("reason", "Mapped dimensions overlap with (unmapped) dimensions")
                            .addData("dimensions", builder._dimensions)
                            .addData("mappedDimensions", builder._mappedDimensions)
                            .log();
                    return false;
                }

                // Assert that the period dimension name is not overlapping
                if (mappedTargetDimensions.contains(builder._periodDimensionName)) {
                    LOGGER.warn()
                            .setMessage("Invalid PeriodicStatisticsSink")
                            .addData("reason", "Mapped dimensions overlap with periodDimensionName")
                            .addData("periodDimensionName", builder._periodDimensionName)
                            .addData("mappedDimensions", builder._mappedDimensions)
                            .log();
                    return false;
                }
                if (builder._dimensions.contains(builder._periodDimensionName)) {
                    LOGGER.warn()
                            .setMessage("Invalid PeriodicStatisticsSink")
                            .addData("reason", "(Unmapped) dimensions overlap with periodDimensionName")
                            .addData("periodDimensionName", builder._periodDimensionName)
                            .addData("dimensions", builder._dimensions)
                            .log();
                    return false;
                }

                // NOTE: This does mean that a logically equivalent overlap would be
                // disallowed. For example:
                //
                // dimensions = {"a", "b"}
                // mappedDimensions = {"b": "b"}
                //
                // This is logically valid because the dimension rule for "b" and the
                // mapped dimension rule for "b" are equivalent! However, the current
                // check disallows this.
                //
                // See test:
                // testValidationDimensionCollisionAlthoughLogicallyEquivalent
                //
                // TODO(ville): Once we have a use case address this.

                return true;
            }
        }

        private static final class CheckDefaultDimensionTargets implements CheckWithCheck.SimpleCheck {

            private static final long serialVersionUID = 5011108547193627318L;

            @Override
            public boolean isSatisfied(final Object validatedObject, final Object value) {
                // TODO(ville): Find a way to throw validation exceptions instead of logging.

                if (!(validatedObject instanceof PeriodicStatisticsSink.Builder)) {
                    return false;
                }
                final PeriodicStatisticsSink.Builder builder = (PeriodicStatisticsSink.Builder) validatedObject;

                for (final String keyToDefault : builder._defaultDimensionValues.keySet()) {
                    if (!builder._dimensions.contains(keyToDefault) && !builder._mappedDimensions.containsKey(keyToDefault)) {
                        LOGGER.warn()
                                .setMessage("Invalid PeriodicStatisticsSink")
                                .addData("reason", "Default dimensions key not specified in (unmapped) dimensions or mapped dimensions")
                                .addData("dimensions", builder._dimensions)
                                .addData("mappedDimensions.keySet", builder._mappedDimensions.keySet())
                                .addData("defaultDimensionKey", keyToDefault)
                                .log();
                        return false;
                    }
                }

                return true;
            }
        }
    }
}
