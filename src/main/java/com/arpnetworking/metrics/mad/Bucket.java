/*
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
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.Accumulator;
import com.arpnetworking.metrics.mad.model.statistics.Calculator;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

/**
* Contains samples for a particular aggregation period in time.
*
* @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
*/
/* package private */ final class Bucket {

    /**
     * Close the bucket. The aggregates for each metric are emitted to the sink.
     */
    public void close() {
        // Set the close flag before acquiring the write lock to allow "readers" to fail fast
        if (_isOpen) {
            _isOpen = false;
            final ImmutableMultimap.Builder<String, AggregatedData> data = ImmutableMultimap.builder();
            computeStatistics(_counterMetricCalculators, _specifiedCounterStatistics, data);
            computeStatistics(_gaugeMetricCalculators, _specifiedGaugeStatistics, data);
            computeStatistics(_timerMetricCalculators, _specifiedTimerStatistics, data);
            computeStatistics(_explicitMetricCalculators, _specifiedStatisticsCache, data);
            final PeriodicData periodicData = ThreadLocalBuilder.build(
                    PeriodicData.Builder.class,
                    b -> b.setData(data.build())
                            .setDimensions(_key)
                            .setPeriod(_period)
                            .setStart(_start)
                            .setMinRequestTime(_minRequestTime.orElse(null)));
            _sink.recordAggregateData(periodicData);
        } else {
            LOGGER.warn()
                    .setMessage("Bucket closed multiple times")
                    .addData("bucket", this)
                    .log();
        }
    }

    /**
     * Add data in the form of a {@link Record} to this {@link Bucket}.
     *
     * @param record The data to add to this {@link Bucket}.
     */
    public void add(final Record record) {
        if (!_isOpen) {
            // TODO(vkoskela): Re-aggregation starts here.
            // 1) Send the record back to Aggregator.
            // 2) This causes a new bucket to be created for this start+period.
            // 3) Enhance aggregation at edges to support re-aggregation (or prevent overwrite).
            BUCKET_CLOSED_LOGGER
                    .warn()
                    .setMessage("Discarding metric")
                    .addData("reason", "added after close")
                    .addData("record", record)
                    .log();
            return;
        }

        for (final Map.Entry<String, ? extends Metric> entry : record.getMetrics().entrySet()) {
            final String name = entry.getKey();
            final Metric metric = entry.getValue();

            if (metric.getValues().isEmpty() && metric.getStatistics().isEmpty()) {
                LOGGER.debug()
                        .setMessage("Discarding metric")
                        .addData("reason", "no samples or statistics")
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
                throw new RuntimeException(e);
            }
            if (specifiedStatistics.isPresent()) {
                final Optional<ImmutableSet<Statistic>> dependentStatistics;
                try {
                    dependentStatistics = _dependentStatisticsCache.get(name);
                } catch (final ExecutionException e) {
                    throw new RuntimeException(e);
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
            addMetric(metric, calculators);
        }

        updateMinRequestTime(record);
    }

    private void updateMinRequestTime(final Record record) {
        if (record.getRequestTime().isPresent()) {
            if (!_minRequestTime.isPresent()) {
                _minRequestTime = record.getRequestTime();
            }
            if (record.getRequestTime().get().isBefore(_minRequestTime.get())) {
                _minRequestTime = record.getRequestTime();
            }
        }
    }

    public ZonedDateTime getStart() {
        return _start;
    }

    public boolean isOpen() {
        return _isOpen;
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
                .put("key", _key)
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

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void computeStatistics(
            final Map<String, Collection<Calculator<?>>> calculatorsByMetric,
            final LoadingCache<String, Optional<ImmutableSet<Statistic>>> specifiedStatistics,
            final ImmutableMultimap.Builder<String, AggregatedData> data) {
        computeStatistics(calculatorsByMetric, (metric, statistic) -> {
            final Optional<ImmutableSet<Statistic>> stats;
            try {
                stats = specifiedStatistics.get(metric);
            } catch (final ExecutionException e) {
                throw new RuntimeException(e);
            }
            return stats.isPresent() && stats.get().contains(statistic);
        }, data);
    }

    private void computeStatistics(
            final Map<String, Collection<Calculator<?>>> calculatorsByMetric,
            final ImmutableSet<Statistic> specifiedStatistics,
            final ImmutableMultimap.Builder<String, AggregatedData> data) {
        computeStatistics(calculatorsByMetric, (metric, statistic) -> specifiedStatistics.contains(statistic), data);
    }
    private void computeStatistics(
            final Map<String, Collection<Calculator<?>>> calculatorsByMetric,
            final BiFunction<String, Statistic, Boolean> specified,
            final ImmutableMultimap.Builder<String, AggregatedData> data) {

        for (final Map.Entry<String, Collection<Calculator<?>>> entry : calculatorsByMetric.entrySet()) {
            final String metric = entry.getKey();
            final Collection<Calculator<?>> calculators = entry.getValue();

            // Build calculator dependencies for metric
            // TODO(vkoskela): This is a waste of time. [NEXT]
            // - Single set of calculators per metric and per statistic (no distinction between non-aux and aux)
            // - Check the set of statistics to see if the calculator should be published
            // - Still need both sets of statistics in order to create new set of calculators
            final Map<Statistic, Calculator<?>> dependencies = Maps.newHashMap();
            Optional<Calculator<?>> countStatisticCalculator = Optional.empty();
            for (final Calculator<?> calculator : calculators) {
                if (COUNT_STATISTIC.equals(calculator.getStatistic())) {
                    countStatisticCalculator = Optional.of(calculator);
                }
                dependencies.put(calculator.getStatistic(), calculator);
            }
            final CalculatedValue<?> populationSize;
            if (countStatisticCalculator.isPresent()) {
                populationSize = countStatisticCalculator.get().calculate(dependencies);
            } else {
                populationSize = ThreadLocalBuilder.<
                        CalculatedValue<Object>,
                        CalculatedValue.Builder<Object>>buildGeneric(
                                CalculatedValue.Builder.class,
                                b -> b.setValue(
                                        ThreadLocalBuilder.build(
                                                DefaultQuantity.Builder.class,
                                                builder -> builder.setValue(-1.0))));
            }

            // Compute each calculated value requested by the client
            for (final Calculator<?> calculator : calculators) {
                final CalculatedValue<?> calculatedValue = calculator.calculate(dependencies);
                final AggregatedData datum = ThreadLocalBuilder.build(
                        AggregatedData.Builder.class,
                        b -> b.setSupportingData(null)
                                .setValue(calculatedValue.getValue())
                                .setIsSpecified(specified.apply(metric, calculator.getStatistic()))
                                .setPopulationSize((long) populationSize.getValue().getValue())
                                .setSupportingData(calculatedValue.getData())
                                .setStatistic(calculator.getStatistic()));
                data.put(metric, datum);
            }
        }
    }

    private void addMetric(
            final Metric metric,
            final Collection<Calculator<?>> calculators) {

        // Add the metric data to any accumulators
        for (final Calculator<?> calculator : calculators) {
            final Statistic statistic = calculator.getStatistic();
            if (calculator instanceof Accumulator) {
                final Accumulator<?> accumulator = (Accumulator<?>) calculator;
                synchronized (accumulator) {
                    for (final Quantity quantity : metric.getValues()) {
                        accumulator.accumulate(quantity);
                    }
                    final ImmutableList<CalculatedValue<?>> statisticValue = metric.getStatistics()
                            .getOrDefault(statistic, ImmutableList.of());
                    assert statisticValue != null;
                    for (final CalculatedValue<?> value : statisticValue) {
                        accumulator.accumulateAny(value);
                    }
                }
            }
        }
    }

    private Collection<Calculator<?>> getOrCreateCalculators(
            final String name,
            final Collection<Statistic> specifiedStatistics,
            final Collection<Statistic> dependentStatistics,
            final Map<String, Collection<Calculator<?>>> calculatorsByMetric) {
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
        _key = builder._key;
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

    private boolean _isOpen = true;
    private Optional<ZonedDateTime> _minRequestTime = Optional.empty();

    private final Map<String, Collection<Calculator<?>>> _counterMetricCalculators = Maps.newHashMap();
    private final Map<String, Collection<Calculator<?>>> _gaugeMetricCalculators = Maps.newHashMap();
    private final Map<String, Collection<Calculator<?>>> _timerMetricCalculators = Maps.newHashMap();
    private final Map<String, Collection<Calculator<?>>> _explicitMetricCalculators = Maps.newHashMap();
    private final Sink _sink;
    private final Key _key;
    private final ZonedDateTime _start;
    private final Duration _period;
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
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link Bucket}.
     */
    public static final class Builder extends OvalBuilder<Bucket> {

        /**
         * Public constructor.
         */
        /* package private */ Builder() {
            super(Bucket::new);
        }

        /**
         * Set the key. Cannot be null or empty.
         *
         * @param value The key.
         * @return This {@link Builder} instance.
         */
        public Builder setKey(final Key value) {
            _key = value;
            return this;
        }

        /**
         * Set the sink. Cannot be null or empty.
         *
         * @param value The sink.
         * @return This {@link Builder} instance.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * Set the specified timer statistics. Cannot be null or empty.
         *
         * @param value The specified timer statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setSpecifiedTimerStatistics(final ImmutableSet<Statistic> value) {
            _specifiedTimerStatistics = value;
            return this;
        }

        /**
         * Set the specified counter statistics. Cannot be null or empty.
         *
         * @param value The specified counter statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setSpecifiedCounterStatistics(final ImmutableSet<Statistic> value) {
            _specifiedCounterStatistics = value;
            return this;
        }

        /**
         * Set the specified gauge statistics. Cannot be null or empty.
         *
         * @param value The specified gauge statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setSpecifiedGaugeStatistics(final ImmutableSet<Statistic> value) {
            _specifiedGaugeStatistics = value;
            return this;
        }

        /**
         * Set the dependent timer statistics. Cannot be null or empty.
         *
         * @param value The dependent timer statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setDependentTimerStatistics(final ImmutableSet<Statistic> value) {
            _dependentTimerStatistics = value;
            return this;
        }

        /**
         * Set the dependent counter statistics. Cannot be null or empty.
         *
         * @param value The dependent counter statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setDependentCounterStatistics(final ImmutableSet<Statistic> value) {
            _dependentCounterStatistics = value;
            return this;
        }

        /**
         * Set the dependent gauge statistics. Cannot be null or empty.
         *
         * @param value The dependent gauge statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setDependentGaugeStatistics(final ImmutableSet<Statistic> value) {
            _dependentGaugeStatistics = value;
            return this;
        }

        /**
         * Set the start. Cannot be null or empty.
         *
         * @param value The start.
         * @return This {@link Builder} instance.
         */
        public Builder setStart(final ZonedDateTime value) {
            _start = value;
            return this;
        }

        /**
         * Set the period. Cannot be null or empty.
         *
         * @param value The period.
         * @return This {@link Builder} instance.
         */
        public Builder setPeriod(final Duration value) {
            _period = value;
            return this;
        }

        /**
         * Set the specified statistics for a given metric. Cannot be null or empty.
         *
         * @param value The dependent timer statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setSpecifiedStatistics(final LoadingCache<String, Optional<ImmutableSet<Statistic>>> value) {
            _specifiedStatistics = value;
            return this;
        }

        /**
         * Set the dependent statistics for a given metric. Cannot be null or empty.
         *
         * @param value The dependent timer statistics.
         * @return This {@link Builder} instance.
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
        public Object toLogValue() {
            return LogValueMapFactory.builder(this)
                    .put("sink", _sink)
                    .put("key", _key)
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

        @Override
        public String toString() {
            return toLogValue().toString();
        }

        @NotNull
        private Sink _sink;
        @NotNull
        private Key _key;
        @NotNull
        private ZonedDateTime _start;
        @NotNull
        private Duration _period;
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
