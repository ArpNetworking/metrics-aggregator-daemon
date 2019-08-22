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
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.tsdcore.model.CalculatedValue;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Counts the entries. Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@Loggable
public final class CountStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "count";
    }

    @Override
    public Set<String> getAliases() {
        return Collections.singleton("n");
    }

    @Override
    public Calculator<Void> createCalculator() {
        return new CountAccumulator(this);
    }

    private CountStatistic() { }

    private static final long serialVersionUID = 983762187313397225L;

    /**
     * Accumulator computing the count of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    private static final class CountAccumulator extends BaseCalculator<Void> implements Accumulator<Void> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>Statistic</code>.
         */
        private CountAccumulator(final Statistic statistic) {
            super(statistic);
        }

        @Override
        public Accumulator<Void> accumulate(final Quantity quantity) {
            ++_count;
            return this;
        }

        @Override
        public Accumulator<Void> accumulate(final CalculatedValue<Void> calculatedValue) {
            _count += calculatedValue.getValue().getValue();
            return this;
        }

        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                    CalculatedValue.Builder.class,
                    b1 -> b1.setValue(
                            ThreadLocalBuilder.build(
                                    DefaultQuantity.Builder.class,
                                    b2 -> b2.setValue((double) _count))));
        }

        private long _count = 0;
    }
}
