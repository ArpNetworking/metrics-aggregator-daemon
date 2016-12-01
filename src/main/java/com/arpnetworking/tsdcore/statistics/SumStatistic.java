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
import com.arpnetworking.tsdcore.model.Unit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Takes the sum of the entries. Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
@Loggable
public final class SumStatistic extends BaseStatistic {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "sum";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Calculator<Void> createCalculator() {
        return new SumAccumulator(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculate(final List<Quantity> unorderedValues) {
        double sum = 0d;
        Optional<Unit> unit = Optional.empty();
        for (final Quantity sample : unorderedValues) {
            sum += sample.getValue();
            unit = Optional.ofNullable(unit.orElse(sample.getUnit().orElse(null)));
        }
        return new Quantity.Builder().setValue(sum).setUnit(unit.orElse(null)).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        double sum = 0;
        Optional<Unit> unit = Optional.empty();
        for (final AggregatedData aggregation : aggregations) {
            sum += aggregation.getValue().getValue();
            unit = Optional.ofNullable(unit.orElse(aggregation.getValue().getUnit().orElse(null)));
        }
        return new Quantity.Builder().setValue(sum).setUnit(unit.orElse(null)).build();
    }

    private SumStatistic() { }

    private static final long serialVersionUID = -1534109546290882210L;

    /**
     * Accumulator computing the sum of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class SumAccumulator extends BaseCalculator<Void> implements Accumulator<Void> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>Statistic</code>.
         */
        public SumAccumulator(final Statistic statistic) {
            super(statistic);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            if (_sum.isPresent()) {
                _sum = Optional.of(_sum.get().add(quantity));
            } else {
                _sum = Optional.of(quantity);
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
                    .setValue(_sum.orElse(null))
                    .build();
        }

        private Optional<Quantity> _sum = Optional.empty();
    }
}
