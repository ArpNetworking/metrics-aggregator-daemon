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
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.arpnetworking.tsdcore.statistics.Accumulator;
import com.arpnetworking.tsdcore.statistics.Calculator;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

/**
* Contains samples for a particular aggregation period in time.
*
* @author Ville Koskela (vkoskela at groupon dot com)
*/
/* package private */ final class Bucket {

    /**
     * Close the bucket. The aggregates for each metric are emitted to the sink.
     */
    public void close() {
        // Set the close flag before acquiring the write lock to allow "readers" to fail fast
        if (_isOpen.getAndSet(false)) {
            try {
                // Acquire the write lock and flush the calculated statistics
                _addCloseLock.writeLock().lock();
                final ImmutableList.Builder<AggregatedData> data = ImmutableList.builder();
                computeStatistics(_counterMetricCalculators, _specifiedCounterStatistics, data);
                computeStatistics(_gaugeMetricCalculators, _specifiedGaugeStatistics, data);
                computeStatistics(_timerMetricCalculators, _specifiedTimerStatistics, data);
                computeStatistics(_explicitMetricCalculators, _specifiedStatisticsCache, data);
                // TODO(vkoskela): Perform expression evaluation here. [NEXT]
                // -> This still requires realizing and indexing the computed aggregated data
                // in order to feed the expression evaluation. Once the filtering is consolidated
                // we can probably just build a map here and then do one copy into immutable form
                // in the PeriodicData. This becomes feasible with consolidated filtering because
                // fewer copies (e.g. none) are made downstream.
                // TODO(vkoskela): Perform alert evaluation here. [NEXT]
                // -> This requires expressions. Otherwise, it's just a matter of changing the
                // alerts abstraction from a Sink to something more appropriate and hooking it in
                // here.
                final PeriodicData periodicData = new PeriodicData.Builder()
                        .setData(data.build())
                        .setDimensions(ImmutableMap.of("host", _host))
                        .setPeriod(_period)
                        .setStart(_start)
                        .build();
                _sink.recordAggregateData(periodicData);
            } finally {
                _addCloseLock.writeLock().unlock();
            }
        } else {
            LOGGER.warn()
                    .setMessage("Bucket closed multiple times")
                    .addData("bucket", this)
                    .log();
        }
    }

    /**
     * Add data in the form of a <code>Record</code> to this <code>Bucket</code>.
     *
     * @param record The data to add to this <code>Bucket</code>.
     */
    public void add(final Record record) {
        for (final Map.Entry<String, ? extends Metric> entry : record.getMetrics().entrySet()) {
            final String name = entry.getKey();
            final Metric metric = entry.getValue();

            if (metric.getValues().isEmpty()) {
                LOGGER.debug()
                        .setMessage("Discarding metric")
                        .addData("reason", "no samples")
                        .addData("name", name)
                        .addData("metric", metric)
                        .log();
                continue;
            }

            Collection<Calculator<?>> calculators = Collections.emptyList();
            // First check to see if the user has specified a set of statistics for this metric
            final Optional<ImmutableSet<Statistic>> specifiedStatistics;
            try {
                specifiedStatistics = _specifiedStatisticsCache.get(name);
            } catch (final ExecutionException e) {
                throw Throwables.propagate(e);
            }
            if (specifiedStatistics.isPresent()) {
                final Optional<ImmutableSet<Statistic>> dependentStatistics;
                try {
                    dependentStatistics = _dependentStatisticsCache.get(name);
                } catch (final ExecutionException e) {
                    throw Throwables.propagate(e);
                }
                calculators = getOrCreateCalculators(
                        name,
                        specifiedStatistics.get(),
                        dependentStatistics.get(),
                        _explicitMetricCalculators);
            } else {
                switch (metric.getType()) {
                    case COUNTER: {
                        calculators = getOrCreateCalculators(
                                name,
                                _specifiedCounterStatistics,
                                _dependentCounterStatistics,
                                _counterMetricCalculators);
                        break;
                    }
                    case GAUGE: {
                        calculators = getOrCreateCalculators(
                                name,
                                _specifiedGaugeStatistics,
                                _dependentGaugeStatistics,
                                _gaugeMetricCalculators);
                        break;
                    }
                    case TIMER: {
                        calculators = getOrCreateCalculators(
                                name,
                                _specifiedTimerStatistics,
                                _dependentTimerStatistics,
                                _timerMetricCalculators);
                        break;
                    }
                    default:
                        LOGGER.warn()
                                .setMessage("Discarding metric")
                                .addData("reason", "unsupported type")
                                .addData("name", name)
                                .addData("metric", metric)
                                .log();
                }
            }
            addMetric(
                    name,
                    metric,
                    record.getTime(),
                    calculators);
        }
    }

    public DateTime getStart() {
        return _start;
    }

    public boolean isOpen() {
        return _isOpen.get();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("isOpen", _isOpen)
                .put("sink", _sink)
                .put("service", _service)
                .put("cluster", _cluster)
                .put("host", _host)
                .put("start", _start)
                .put("period", _period)
                .put("timerStatistics", _specifiedTimerStatistics)
                .put("counterStatistics", _specifiedCounterStatistics)
                .put("gaugeStatistics", _specifiedGaugeStatistics)
                .put("timerStatistics", _specifiedTimerStatistics)
                .put("counterStatistics", _specifiedCounterStatistics)
                .put("gaugeStatistics", _specifiedGaugeStatistics)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void computeStatistics(
            final ConcurrentMap<String, Collection<Calculator<?>>> calculatorsByMetric,
            final LoadingCache<String, Optional<ImmutableSet<Statistic>>> specifiedStatistics,
            final ImmutableList.Builder<AggregatedData> data) {
        computeStatistics(calculatorsByMetric, (metric, statistic) -> {
            final Optional<ImmutableSet<Statistic>> stats;
            try {
                stats = specifiedStatistics.get(metric);
            } catch (final ExecutionException e) {
                throw Throwables.propagate(e);
            }
            return stats.isPresent() && stats.get().contains(statistic);
        }, data);
    }

    private void computeStatistics(
            final ConcurrentMap<String, Collection<Calculator<?>>> calculatorsByMetric,
            final ImmutableSet<Statistic> specifiedStatistics,
            final ImmutableList.Builder<AggregatedData> data) {
        computeStatistics(calculatorsByMetric, (metric, statistic) -> specifiedStatistics.contains(statistic), data);
    }
    private void computeStatistics(
            final ConcurrentMap<String, Collection<Calculator<?>>> calculatorsByMetric,
            final BiFunction<String, Statistic, Boolean> specified,
            final ImmutableList.Builder<AggregatedData> data) {

        final FQDSN.Builder fqdsnBuilder = new FQDSN.Builder()
                .setCluster(_cluster)
                .setService(_service);
        final AggregatedData.Builder datumBuilder = new AggregatedData.Builder()
                .setStart(_start)
                .setPeriod(_period)
                .setHost(_host);

        for (final Map.Entry<String, Collection<Calculator<?>>> entry : calculatorsByMetric.entrySet()) {
            final String metric = entry.getKey();
            final Collection<Calculator<?>> calculators = entry.getValue();

            // Build calculator dependencies for metric
            // TODO(vkoskela): This is a waste of time. [NEXT]
            // - Single set of calculators per metric and per statistic (no distinction between non-aux and aux)
            // - Check the set of statistics to see if the calculator should be published
            // - Still need both sets of statistics in order to create new set of calculators
            final Map<Statistic, Calculator<?>> dependencies = Maps.newHashMap();
            Optional<Calculator<?>> countStatisticCalculator = Optional.absent();
            for (final Calculator<?> calculator : calculators) {
                if (COUNT_STATISTIC.equals(calculator.getStatistic())) {
                    countStatisticCalculator = Optional.of(calculator);
                }
                dependencies.put(calculator.getStatistic(), calculator);
            }
            CalculatedValue<?> populationSize = new CalculatedValue.Builder<>()
                    .setValue(new Quantity.Builder().setValue(-1.0).build())
                    .build();
            if (countStatisticCalculator.isPresent()) {
                populationSize = countStatisticCalculator.get().calculate(dependencies);
            }

            // Compute each calculated value requested by the client
            fqdsnBuilder.setMetric(metric);
            for (final Calculator<?> calculator : calculators) {
                datumBuilder.setFQDSN(
                        fqdsnBuilder.setStatistic(calculator.getStatistic())
                                .build());

                datumBuilder.setSupportingData(null);
                final CalculatedValue<?> calculatedValue = calculator.calculate(dependencies);
                data.add(
                        datumBuilder.setValue(calculatedValue.getValue())
                                .setIsSpecified(specified.apply(metric, calculator.getStatistic()))
                                .setPopulationSize((long) populationSize.getValue().getValue())
                                .setSupportingData(calculatedValue.getData())
                                .build());
            }
        }
    }

    private void addMetric(
            final String name,
            final Metric metric,
            final DateTime time,
            final Collection<Calculator<?>> calculators) {

        try {
            // Acquire a read lock and validate the bucket is still open
            _addCloseLock.readLock().lock();
            if (!_isOpen.get()) {
                // TODO(vkoskela): Re-aggregation starts here.
                // 1) Send the record back to Aggregator.
                // 2) This causes a new bucket to be created for this start+period.
                // 3) Enhance aggregation at edges to support re-aggregation (or prevent overwrite).
                BUCKET_CLOSED_LOGGER
                        .warn()
                        .setMessage("Discarding metric")
                        .addData("reason", "added after close")
                        .addData("name", name)
                        .addData("metric", metric)
                        .addData("time", time)
                        .log();
                return;
            }

            // Add the value to any accumulators
            for (final Calculator<?> calculator : calculators) {
                if (calculator instanceof Accumulator) {
                    final Accumulator<?> accumulator = (Accumulator<?>) calculator;
                    synchronized (accumulator) {
                        for (final Quantity quantity : metric.getValues()) {
                            accumulator.accumulate(quantity);
                        }
                    }
                }
            }
        } finally {
            _addCloseLock.readLock().unlock();
        }
    }

    private Collection<Calculator<?>> getOrCreateCalculators(
            final String name,
            final Collection<Statistic> specifiedStatistics,
            final Collection<Statistic> dependentStatistics,
            final ConcurrentMap<String, Collection<Calculator<?>>> calculatorsByMetric) {
        Collection<Calculator<?>> calculators = calculatorsByMetric.get(name);
        if (calculators == null) {
            final Set<Calculator<?>> newCalculators = Sets.newHashSet();
            for (final Statistic statistic : specifiedStatistics) {
                newCalculators.add(statistic.createCalculator());
            }
            for (final Statistic statistic : dependentStatistics) {
                newCalculators.add(statistic.createCalculator());
            }
            newCalculators.add(COUNT_STATISTIC.createCalculator());
            calculators = calculatorsByMetric.putIfAbsent(name, newCalculators);
            if (calculators == null) {
                calculators = newCalculators;
            }
        }
        return calculators;
    }

    Bucket(final Builder builder) {
        _sink = builder._sink;
        _cluster = builder._cluster;
        _service = builder._service;
        _host = builder._host;
        _start = builder._start;
        _period = builder._period;
        _specifiedCounterStatistics = builder._specifiedCounterStatistics;
        _specifiedGaugeStatistics = builder._specifiedGaugeStatistics;
        _specifiedTimerStatistics = builder._specifiedTimerStatistics;
        _dependentCounterStatistics = builder._dependentCounterStatistics;
        _dependentGaugeStatistics = builder._dependentGaugeStatistics;
        _dependentTimerStatistics = builder._dependentTimerStatistics;
        _specifiedStatisticsCache = builder._specifiedStatistics;
        _dependentStatisticsCache = builder._dependentStatistics;
    }

    private final AtomicBoolean _isOpen = new AtomicBoolean(true);
    private final ReadWriteLock _addCloseLock = new ReentrantReadWriteLock();
    private final ConcurrentMap<String, Collection<Calculator<?>>> _counterMetricCalculators = Maps.newConcurrentMap();
    private final ConcurrentMap<String, Collection<Calculator<?>>> _gaugeMetricCalculators = Maps.newConcurrentMap();
    private final ConcurrentMap<String, Collection<Calculator<?>>> _timerMetricCalculators = Maps.newConcurrentMap();
    private final ConcurrentMap<String, Collection<Calculator<?>>> _explicitMetricCalculators = Maps.newConcurrentMap();
    private final Sink _sink;
    private final String _cluster;
    private final String _service;
    private final String _host;
    private final DateTime _start;
    private final Period _period;
    private final ImmutableSet<Statistic> _specifiedCounterStatistics;
    private final ImmutableSet<Statistic> _specifiedGaugeStatistics;
    private final ImmutableSet<Statistic> _specifiedTimerStatistics;
    private final ImmutableSet<Statistic> _dependentCounterStatistics;
    private final ImmutableSet<Statistic> _dependentGaugeStatistics;
    private final ImmutableSet<Statistic> _dependentTimerStatistics;
    private final LoadingCache<String, Optional<ImmutableSet<Statistic>>> _dependentStatisticsCache;
    private final LoadingCache<String, Optional<ImmutableSet<Statistic>>> _specifiedStatisticsCache;

    private static final StatisticFactory STATISTIC_FACTORY;
    private static final Statistic COUNT_STATISTIC;
    private static final Logger LOGGER = LoggerFactory.getLogger(Bucket.class);
    private static final Logger BUCKET_CLOSED_LOGGER = LoggerFactory.getRateLimitLogger(Bucket.class, Duration.ofSeconds(30));

    static {
        STATISTIC_FACTORY = new StatisticFactory();
        COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");
    }

    /**
     * <code>Builder</code> implementation for <code>Bucket</code>.
     */
    public static final class Builder extends OvalBuilder<Bucket> {

        /**
         * Public constructor.
         */
        /* package private */ Builder() {
            super(Bucket::new);
        }

        /**
         * Set the service. Cannot be null or empty.
         *
         * @param value The service.
         * @return This <code>Builder</code> instance.
         */
        public Builder setService(final String value) {
            _service = value;
            return this;
        }

        /**
         * Set the cluster. Cannot be null or empty.
         *
         * @param value The cluster.
         * @return This <code>Builder</code> instance.
         */
        public Builder setCluster(final String value) {
            _cluster = value;
            return this;
        }

        /**
         * Set the host. Cannot be null or empty.
         *
         * @param value The host.
         * @return This <code>Builder</code> instance.
         */
        public Builder setHost(final String value) {
            _host = value;
            return this;
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
         * Set the specified timer statistics. Cannot be null or empty.
         *
         * @param value The specified timer statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSpecifiedTimerStatistics(final ImmutableSet<Statistic> value) {
            _specifiedTimerStatistics = value;
            return this;
        }

        /**
         * Set the specified counter statistics. Cannot be null or empty.
         *
         * @param value The specified counter statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSpecifiedCounterStatistics(final ImmutableSet<Statistic> value) {
            _specifiedCounterStatistics = value;
            return this;
        }

        /**
         * Set the specified gauge statistics. Cannot be null or empty.
         *
         * @param value The specified gauge statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSpecifiedGaugeStatistics(final ImmutableSet<Statistic> value) {
            _specifiedGaugeStatistics = value;
            return this;
        }

        /**
         * Set the dependent timer statistics. Cannot be null or empty.
         *
         * @param value The dependent timer statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDependentTimerStatistics(final ImmutableSet<Statistic> value) {
            _dependentTimerStatistics = value;
            return this;
        }

        /**
         * Set the dependent counter statistics. Cannot be null or empty.
         *
         * @param value The dependent counter statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDependentCounterStatistics(final ImmutableSet<Statistic> value) {
            _dependentCounterStatistics = value;
            return this;
        }

        /**
         * Set the dependent gauge statistics. Cannot be null or empty.
         *
         * @param value The dependent gauge statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDependentGaugeStatistics(final ImmutableSet<Statistic> value) {
            _dependentGaugeStatistics = value;
            return this;
        }

        /**
         * Set the start. Cannot be null or empty.
         *
         * @param value The start.
         * @return This <code>Builder</code> instance.
         */
        public Builder setStart(final DateTime value) {
            _start = value;
            return this;
        }

        /**
         * Set the period. Cannot be null or empty.
         *
         * @param value The period.
         * @return This <code>Builder</code> instance.
         */
        public Builder setPeriod(final Period value) {
            _period = value;
            return this;
        }

        /**
         * Set the specified statistics for a given metric. Cannot be null or empty.
         *
         * @param value The dependent timer statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSpecifiedStatistics(final LoadingCache<String, Optional<ImmutableSet<Statistic>>> value) {
            _specifiedStatistics = value;
            return this;
        }

        /**
         * Set the dependent statistics for a given metric. Cannot be null or empty.
         *
         * @param value The dependent timer statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDependentStatistics(final LoadingCache<String, Optional<ImmutableSet<Statistic>>> value) {
            _dependentStatistics = value;
            return this;
        }


        /**
         * Generate a Steno log compatible representation.
         *
         * @return Steno log compatible representation.
         */
        @LogValue
        @Override
        public Object toLogValue() {
            return LogValueMapFactory.builder(this)
                    .put("_super", super.toLogValue())
                    .put("sink", _sink)
                    .put("service", _service)
                    .put("cluster", _cluster)
                    .put("host", _host)
                    .put("start", _start)
                    .put("period", _period)
                    .put("specifiedTimerStatistics", _specifiedTimerStatistics)
                    .put("specifiedCounterStatistics", _specifiedCounterStatistics)
                    .put("specifiedGaugeStatistics", _specifiedGaugeStatistics)
                    .put("dependentTimerStatistics", _dependentTimerStatistics)
                    .put("dependentCounterStatistics", _dependentCounterStatistics)
                    .put("dependentGaugeStatistics", _dependentGaugeStatistics)
                    .build();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return toLogValue().toString();
        }


        @NotNull
        @NotEmpty
        private String _service;
        @NotNull
        @NotEmpty
        private String _cluster;
        @NotNull
        @NotEmpty
        private String _host;
        @NotNull
        private Sink _sink;
        @NotNull
        private DateTime _start;
        @NotNull
        private Period _period;
        @NotNull
        private ImmutableSet<Statistic> _specifiedTimerStatistics;
        @NotNull
        private ImmutableSet<Statistic> _specifiedCounterStatistics;
        @NotNull
        private ImmutableSet<Statistic> _specifiedGaugeStatistics;
        @NotNull
        private ImmutableSet<Statistic> _dependentTimerStatistics;
        @NotNull
        private ImmutableSet<Statistic> _dependentCounterStatistics;
        @NotNull
        private ImmutableSet<Statistic> _dependentGaugeStatistics;
        @NotNull
        private LoadingCache<String, Optional<ImmutableSet<Statistic>>> _specifiedStatistics;
        @NotNull
        private LoadingCache<String, Optional<ImmutableSet<Statistic>>> _dependentStatistics;
    }
}
