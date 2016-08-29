/**
 * Copyright 2015 Groupon.com
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
package com.arpnetworking.metrics.mad;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.utility.Launchable;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotNull;
import org.joda.time.Period;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Performs aggregation of <code>Record</code> instances per <code>Period</code>.
 * This class is thread safe.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
// NOTE: The _periodWorkerExecutor is accessed both in synchronized lifecycle methods like launch() and shutdown() but
// also non-synchronized methods like notify(). Access to _periodWorkerExecutor does not need to be synchronized.
@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
public final class Aggregator implements Observer, Launchable {

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void launch() {
        LOGGER.debug()
                .setMessage("Launching aggregator")
                .addData("aggregator", this)
                .log();

        _periodWorkers.clear();
        if (!_periods.isEmpty()) {
            _periodWorkerExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "PeriodWorker"));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdown() {
        LOGGER.debug()
                .setMessage("Stopping aggregator")
                .addData("aggregator", this)
                .log();

        for (final List<PeriodWorker> periodCloserList : _periodWorkers.values()) {
            periodCloserList.forEach(com.arpnetworking.metrics.mad.PeriodWorker::shutdown);
        }
        _periodWorkers.clear();
        if (_periodWorkerExecutor != null) {
            _periodWorkerExecutor.shutdown();
            try {
                _periodWorkerExecutor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                LOGGER.warn("Unable to shutdown period worker executor", e);
            }
            _periodWorkerExecutor = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notify(final Observable observable, final Object event) {
        if (!(event instanceof Record)) {
            LOGGER.error()
                    .setMessage("Observed unsupported event")
                    .addData("event", event)
                    .log();
            return;
        }

        final Record record = (Record) event;
        final Map<String, String> legacyDimensions = extractLegacyDimensions(record);

        final List<Record> splitRecords = splitOnDimensions(record);
        for (Record splitRecord : splitRecords) {
            final ImmutableMap.Builder<String, String> dimensionBuilder = ImmutableMap.builder();
            dimensionBuilder.putAll(legacyDimensions);
            dimensionBuilder.putAll(splitRecord.getDimensionValues());

            final Key key = new DefaultKey(dimensionBuilder.build());
            LOGGER.trace()
                .setMessage("Processing record")
                .addData("record", splitRecord)
                .addData("key", key)
                .log();
            for (final PeriodWorker periodWorker : _periodWorkers.computeIfAbsent(key, this::createPeriodWorkers)) {
                periodWorker.record(splitRecord);
            }
        }
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("sink", _sink)
                .put("timerStatistics", _specifiedTimerStatistics)
                .put("counterStatistics", _specifiedCounterStatistics)
                .put("gaugeStatistics", _specifiedGaugeStatistics)
                .put("periodWorkers", _periodWorkers)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private Map<String, String> extractLegacyDimensions(final Record record) {

        final ImmutableMap.Builder<String, String> defaultDimensions = ImmutableMap.builder();
        for (String fred : _hardCodedWhiteList) {
            defaultDimensions.put(fred, record.getAnnotations().get(fred));
        }

        return defaultDimensions.build();
    }

    private List<Record> splitOnDimensions(final Record record) {
        final ImmutableList.Builder<Record> recordList = ImmutableList.builder();

        final Map<Set<String>, Set<String>> recordSeeds = Maps.newHashMap();
        for (String metricName : record.getMetrics().keySet()) {
            // Grab the list of mapped dimension names
            final ImmutableSet<String> metricDimensions = record.getDimensionMappings().getOrDefault(metricName, ImmutableSet.of());
            // Exclude the ones that don't have a value set
            metricDimensions.removeIf(dimensionName -> !record.getDimensionValues().containsKey(dimensionName));
            // Put the metric into a record that represents a unique set of dimension names
            recordSeeds.putIfAbsent(metricDimensions, Sets.newHashSet());
            recordSeeds.get(metricDimensions).add(metricName);
        }

        for (Map.Entry<Set<String>, Set<String>> recordSeed : recordSeeds.entrySet()) {

            final DefaultRecord.Builder recordBuilder = new DefaultRecord.Builder();
            recordBuilder.setId(UUID.randomUUID().toString());
            recordBuilder.setTime(record.getTime());
            recordBuilder.setAnnotations(record.getAnnotations());

            final ImmutableMap.Builder<String, ImmutableSet<String>> dimensionMapBuilder = ImmutableMap.builder();
            for (String metric : recordSeed.getValue()) {
                final ImmutableSet<String> metricDimensions = record.getDimensionMappings().get(metric);
                if (metricDimensions != null && !metricDimensions.isEmpty()) {
                    dimensionMapBuilder.put(metric, metricDimensions);
                }
            }
            recordBuilder.setDimensionMappings(dimensionMapBuilder.build());

            // Reduce the list of dimensions added to the ones that are used to create the bucket
            final ImmutableMap.Builder<String, String> dimensionValueBuilder = ImmutableMap.builder();
            for (String dimensionName : recordSeed.getKey()) {
                dimensionValueBuilder.put(dimensionName, record.getDimensionValues().get(dimensionName));
            }
            recordBuilder.setDimensionValues(dimensionValueBuilder.build());

            final ImmutableMap.Builder<String, Metric> metricMapBuilder = ImmutableMap.builder();
            for (String metric : recordSeed.getValue()) {
                metricMapBuilder.put(metric, record.getMetrics().get(metric));
            }
            recordBuilder.setMetrics(metricMapBuilder.build());

            recordList.add(recordBuilder.build());
        }

        return recordList.build();
    }

    private final Set<String> _hardCodedWhiteList = ImmutableSet.of(
            Key.HOST_DIMENSION_KEY, Key.SERVICE_DIMENSION_KEY, Key.CLUSTER_DIMENSION_KEY
    );

    private List<PeriodWorker> createPeriodWorkers(final Key key) {
        final List<PeriodWorker> periodWorkerList = Lists.newArrayListWithExpectedSize(_periods.size());
        for (final Period period : _periods) {
            final PeriodWorker periodWorker = new PeriodWorker.Builder()
                    .setPeriod(period)
                    .setBucketBuilder(
                            new Bucket.Builder()
                                    .setKey(key)
                                    .setSpecifiedCounterStatistics(_specifiedCounterStatistics)
                                    .setSpecifiedGaugeStatistics(_specifiedGaugeStatistics)
                                    .setSpecifiedTimerStatistics(_specifiedTimerStatistics)
                                    .setDependentCounterStatistics(_dependentCounterStatistics)
                                    .setDependentGaugeStatistics(_dependentGaugeStatistics)
                                    .setDependentTimerStatistics(_dependentTimerStatistics)
                                    .setSpecifiedStatistics(_cachedSpecifiedStatistics)
                                    .setDependentStatistics(_cachedDependentStatistics)
                                    .setPeriod(period)
                                    .setSink(_sink))
                    .build();
            periodWorkerList.add(periodWorker);
            _periodWorkerExecutor.execute(periodWorker);
        }
        LOGGER.info()
                .setMessage("Created period workers")
                .addData("key", key)
                .addData("periodWorkersSize", periodWorkerList.size())
                .log();
        return periodWorkerList;
    }

    private ImmutableSet<Statistic> computeDependentStatistics(final ImmutableSet<Statistic> statistics) {
        final ImmutableSet.Builder<Statistic> builder = ImmutableSet.builder();
        for (final Statistic statistic : statistics) {
            statistic.getDependencies().stream().filter(dependency -> !statistics.contains(dependency)).forEach(builder::add);
        }
        return builder.build();
    }

    private Aggregator(final Builder builder) {
        _periods = ImmutableSet.copyOf(builder._periods);
        _sink = builder._sink;
        _specifiedCounterStatistics = ImmutableSet.copyOf(builder._counterStatistics);
        _specifiedGaugeStatistics = ImmutableSet.copyOf(builder._gaugeStatistics);
        _specifiedTimerStatistics = ImmutableSet.copyOf(builder._timerStatistics);
        _dependentCounterStatistics = computeDependentStatistics(_specifiedCounterStatistics);
        _dependentGaugeStatistics = computeDependentStatistics(_specifiedGaugeStatistics);
        _dependentTimerStatistics = computeDependentStatistics(_specifiedTimerStatistics);
        final ImmutableMap.Builder<Pattern, ImmutableSet<Statistic>> statisticsBuilder = ImmutableMap.builder();
        for (final Map.Entry<String, Set<Statistic>> entry : builder._statistics.entrySet()) {
            final Pattern pattern = Pattern.compile(entry.getKey());
            final ImmutableSet<Statistic> statistics = ImmutableSet.copyOf(entry.getValue());
            statisticsBuilder.put(pattern, statistics);
        }
        _statistics = statisticsBuilder.build();

//        _dimensions = builder._dimensions;

        _cachedSpecifiedStatistics = CacheBuilder
                .newBuilder()
                .concurrencyLevel(1)
                .build(
                        new CacheLoader<String, Optional<ImmutableSet<Statistic>>>() {
                            // TODO(vkoskela): Add @NonNull annotation to metric. [ISSUE-?]
                            @Override
                            public Optional<ImmutableSet<Statistic>> load(final String metric) throws Exception {
                                for (final Map.Entry<Pattern, ImmutableSet<Statistic>> entry : _statistics.entrySet()) {
                                    final Pattern pattern = entry.getKey();
                                    final ImmutableSet<Statistic> statistics = entry.getValue();
                                    if (pattern.matcher(metric).matches()) {
                                        return Optional.of(statistics);
                                    }
                                }
                                return Optional.absent();
                            }
                        });
        _cachedDependentStatistics = CacheBuilder
                .newBuilder()
                .concurrencyLevel(1)
                .build(new CacheLoader<String, Optional<ImmutableSet<Statistic>>>() {
                            // TODO(vkoskela): Add @NonNull annotation to metric. [ISSUE-?]
                            @Override
                            public Optional<ImmutableSet<Statistic>> load(final String metric) throws Exception {
                                final Optional<ImmutableSet<Statistic>> statistics = _cachedSpecifiedStatistics.get(metric);
                                if (statistics.isPresent()) {
                                   return Optional.of(computeDependentStatistics(statistics.get()));
                                } else {
                                   return Optional.absent();
                                }
                           }
                        });
}

    private final ImmutableSet<Period> _periods;
    private final Sink _sink;
    private final ImmutableSet<Statistic> _specifiedTimerStatistics;
    private final ImmutableSet<Statistic> _specifiedCounterStatistics;
    private final ImmutableSet<Statistic> _specifiedGaugeStatistics;
    private final ImmutableSet<Statistic> _dependentTimerStatistics;
    private final ImmutableSet<Statistic> _dependentCounterStatistics;
    private final ImmutableSet<Statistic> _dependentGaugeStatistics;
    private final ImmutableMap<Pattern, ImmutableSet<Statistic>> _statistics;
//    private final Map<String, Map<Pattern, Set<String>>> _dimensions;
    private final LoadingCache<String, Optional<ImmutableSet<Statistic>>> _cachedSpecifiedStatistics;
    private final LoadingCache<String, Optional<ImmutableSet<Statistic>>> _cachedDependentStatistics;
    private final Map<Key, List<PeriodWorker>> _periodWorkers = Maps.newConcurrentMap();

    private ExecutorService _periodWorkerExecutor = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregator.class);

    /**
     * <code>Builder</code> implementation for <code>Aggregator</code>.
     */
    public static final class Builder extends OvalBuilder<Aggregator> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(Aggregator::new);
        }

        /**
         * Set the sink. Cannot be null or empty.
         *
         * @param value The sink.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * Set the periods. Cannot be null or empty.
         *
         * @param value The periods.
         * @return This <code>Builder</code> instance.
         */
        public Builder setPeriods(final Set<Period> value) {
            _periods = value;
            return this;
        }

        /**
         * Set the timer statistics. Cannot be null or empty.
         *
         * @param value The timer statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setTimerStatistics(final Set<Statistic> value) {
            _timerStatistics = value;
            return this;
        }

        /**
         * Set the counter statistics. Cannot be null or empty.
         *
         * @param value The counter statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setCounterStatistics(final Set<Statistic> value) {
            _counterStatistics = value;
            return this;
        }

        /**
         * Set the gauge statistics. Cannot be null or empty.
         *
         * @param value The gauge statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setGaugeStatistics(final Set<Statistic> value) {
            _gaugeStatistics = value;
            return this;
        }

        /**
         * The statistics to compute for a metric pattern. Optional. Cannot be null.
         * Default is empty.
         *
         * @param value The gauge statistics.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setStatistics(final Map<String, Set<Statistic>> value) {
            _statistics = value;
            return this;
        }

        /*
         * dimensions configuration.
         *
         * dimensions Configuration
         * @param value The dimensions configuration
         * @return This instance of <code>Builder</code>
         *
        public Builder setDimensions(final Map<String, Map<Pattern, Set<String>>> value) {
            _dimensions = value;
            return this;
        }
*/
        @NotNull
        private Sink _sink;
        @NotNull
        private Set<Period> _periods;
        @NotNull
        private Set<Statistic> _timerStatistics;
        @NotNull
        private Set<Statistic> _counterStatistics;
        @NotNull
        private Set<Statistic> _gaugeStatistics;
        @NotNull
        private Map<String, Set<Statistic>> _statistics = Collections.emptyMap();
//        @NotNull
//        private Map<String, Map<Pattern, Set<String>>> _dimensions;
    }
}
