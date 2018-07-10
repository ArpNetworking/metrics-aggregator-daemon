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
package com.inscopemetrics.mad.statistics;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.collect.Sets;
import com.inscopemetrics.mad.model.CalculatedValue;
import com.inscopemetrics.mad.model.Quantity;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Min statistic (e.g. top 0th percentile). Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class MinStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "min";
    }

    @Override
    public Set<String> getAliases() {
        return ALIASES;
    }

    @Override
    public Calculator<Void> createCalculator() {
        return new MinAccumulator(this);
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

        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            // Assert: that under the new Quantity normalization the units should always be the same.
            assertUnit(_min.map(Quantity::getUnit).orElse(Optional.empty()), quantity.getUnit(), _min.isPresent());

            if (_min.isPresent()) {
                if (quantity.getValue() < _min.get().getValue()) {
                    _min = Optional.of(quantity);
                }
            } else {
                _min = Optional.of(quantity);
            }
            return this;
        }

        @Override
        public Accumulator<Void> accumulate(final CalculatedValue<Void> calculatedValue) {
            return accumulate(calculatedValue.getValue());
        }

        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                    CalculatedValue.Builder.class,
                    b -> b.setValue(_min.orElse(null)));
        }

        private Optional<Quantity> _min = Optional.empty();
    }
}
