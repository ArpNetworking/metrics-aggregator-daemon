/**
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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Min statistic (e.g. top 0th percentile). Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
@Loggable
public final class MinStatistic extends BaseStatistic {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "min";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAliases() {
        return ALIASES;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Calculator<Void> createCalculator() {
        return new MinAccumulator(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculate(final List<Quantity> unorderedValues) {
        Optional<Quantity> min = Optional.absent();
        for (final Quantity sample : unorderedValues) {
            if (min.isPresent()) {
                if (sample.compareTo(min.get()) < 0) {
                    min = Optional.of(sample);
                }
            } else {
                min = Optional.of(sample);
            }
        }
        return min.get();
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        Optional<Quantity> min = Optional.absent();
        for (final AggregatedData datum : aggregations) {
            if (min.isPresent()) {
                if (datum.getValue().compareTo(min.get()) < 0) {
                    min = Optional.of(datum.getValue());
                }
            } else {
                min = Optional.of(datum.getValue());
            }
        }
        return min.get();
    }

    private MinStatistic() { }

    private static final Set<String> ALIASES;
    private static final long serialVersionUID = 107620025236661457L;

    static {
        ALIASES = Sets.newHashSet();
        ALIASES.add("tp0");
        ALIASES.add("p0");
    }

    /**
     * Accumulator computing the minimum of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class MinAccumulator extends BaseCalculator<Void> implements Accumulator<Void> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>Statistic</code>.
         */
        public MinAccumulator(final Statistic statistic) {
            super(statistic);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            if (_min.isPresent()) {
                if (quantity.compareTo(_min.get()) < 0) {
                    _min = Optional.of(quantity);
                }
            } else {
                _min = Optional.of(quantity);
            }
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Accumulator<Void> accumulate(final CalculatedValue<Void> calculatedValue) {
            return accumulate(calculatedValue.getValue());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return new CalculatedValue.Builder<Void>()
                    .setValue(_min.orNull())
                    .build();
        }

        private Optional<Quantity> _min = Optional.absent();
    }
}
