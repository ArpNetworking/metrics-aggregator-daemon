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
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Max statistic (e.g. top 100th percentile). Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class MaxStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "max";
    }

    @Override
    public Set<String> getAliases() {
        return ALIASES;
    }

    @Override
    public Calculator<Void> createCalculator() {
        return new MaxAccumulator(this);
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

        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            // Assert: that under the new Quantity normalization the units should always be the same.
            assertUnit(_max.map(Quantity::getUnit).orElse(Optional.empty()), quantity.getUnit(), _max.isPresent());

            if (_max.isPresent()) {
                if (quantity.getValue() > _max.get().getValue()) {
                    _max = Optional.of(quantity);
                }
            } else {
                _max = Optional.of(quantity);
            }
            return this;
        }

        @Override
        public Accumulator<Void> accumulate(final CalculatedValue<Void> calculatedValue) {
            return accumulate(calculatedValue.getValue());
        }

        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return new CalculatedValue.Builder<Void>()
                    .setValue(_max.orElse(null))
                    .build();
        }

        private Optional<Quantity> _max = Optional.empty();
    }
}
