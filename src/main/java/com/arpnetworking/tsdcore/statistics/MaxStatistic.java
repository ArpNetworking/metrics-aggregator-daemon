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
 * Max statistic (e.g. top 100th percentile). Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
@Loggable
public final class MaxStatistic extends BaseStatistic {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "max";
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
        return new MaxAccumulator(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculate(final List<Quantity> unorderedValues) {
        Optional<Quantity> max = Optional.absent();
        for (final Quantity sample : unorderedValues) {
            if (max.isPresent()) {
                if (sample.compareTo(max.get()) > 0) {
                    max = Optional.of(sample);
                }
            } else {
                max = Optional.of(sample);
            }
        }
        return max.get();
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        Optional<Quantity> max = Optional.absent();
        for (final AggregatedData datum : aggregations) {
            if (max.isPresent()) {
                if (datum.getValue().compareTo(max.get()) > 0) {
                    max = Optional.of(datum.getValue());
                }
            } else {
                max = Optional.of(datum.getValue());
            }
        }
        return max.get();
    }

    private MaxStatistic() { }

    private static final Set<String> ALIASES;
    private static final long serialVersionUID = 4788356950823429496L;

    static {
        ALIASES = Sets.newHashSet();
        ALIASES.add("tp100");
        ALIASES.add("p100");
    }


    /**
     * Accumulator computing the maximum of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class MaxAccumulator extends BaseCalculator<Void> implements Accumulator<Void> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>Statistic</code>.
         */
        public MaxAccumulator(final Statistic statistic) {
            super(statistic);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            if (_max.isPresent()) {
                if (quantity.compareTo(_max.get()) > 0) {
                    _max = Optional.of(quantity);
                }
            } else {
                _max = Optional.of(quantity);
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
                    .setValue(_max.orNull())
                    .build();
        }

        private Optional<Quantity> _max = Optional.absent();
    }
}
