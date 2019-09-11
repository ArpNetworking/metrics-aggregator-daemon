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
package com.arpnetworking.metrics.mad.model.statistics;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.tsdcore.model.CalculatedValue;

import java.util.Map;
import java.util.Optional;

/**
 * Takes the sum of the entries. Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@Loggable
public final class SumStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "sum";
    }

    @Override
    public Calculator<Void> createCalculator() {
        return new SumAccumulator(this);
    }

    private SumStatistic() { }

    private static final long serialVersionUID = -1534109546290882210L;

    /**
     * Accumulator computing the sum of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
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

        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            // Assert: that under the new Quantity normalization the units should always be the same.
            assertUnit(_sum.map(Quantity::getUnit).orElse(Optional.empty()), quantity.getUnit(), _sum.isPresent());

            if (_sum.isPresent()) {
                _sum = Optional.of(_sum.get().add(quantity));
            } else {
                _sum = Optional.of(quantity);
            }
            return this;
        }

        @Override
        public Accumulator<Void> accumulate(final CalculatedValue<Void> calculatedValue) {
            return accumulateAny(calculatedValue);
        }

        @Override
        public Accumulator<Void> accumulateAny(final CalculatedValue<?> calculatedValue) {
            return accumulate(calculatedValue.getValue());
        }

        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                    CalculatedValue.Builder.class,
                    b -> b.setValue(_sum.orElse(null)));
        }

        private Optional<Quantity> _sum = Optional.empty();
    }
}
