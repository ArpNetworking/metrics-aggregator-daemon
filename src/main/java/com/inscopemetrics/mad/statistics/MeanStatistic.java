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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.inscopemetrics.mad.model.CalculatedValue;

import java.util.Map;
import java.util.Set;

/**
 * Takes the mean of the entries. Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class MeanStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "mean";
    }

    @Override
    public Calculator<Void> createCalculator() {
        return new MeanCalculator(this);
    }

    @Override
    public Set<Statistic> getDependencies() {
        return DEPENDENCIES.get();
    }

    private MeanStatistic() { }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Supplier<Statistic> SUM_STATISTIC =
            Suppliers.memoize(() -> STATISTIC_FACTORY.getStatistic("sum"));
    private static final Supplier<Statistic> COUNT_STATISTIC =
            Suppliers.memoize(() -> STATISTIC_FACTORY.getStatistic("count"));
    private static final Supplier<Set<Statistic>> DEPENDENCIES =
            Suppliers.memoize(() -> ImmutableSet.of(SUM_STATISTIC.get(), COUNT_STATISTIC.get()));
    private static final long serialVersionUID = 2943082617025777130L;

    /**
     * Calculator computing the average.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class MeanCalculator extends BaseCalculator<Void> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>Statistic</code>.
         */
        public MeanCalculator(final Statistic statistic) {
            super(statistic);
        }

        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            final CalculatedValue<?> sum = dependencies.get(SUM_STATISTIC.get()).calculate(dependencies);
            final CalculatedValue<?> count = dependencies.get(COUNT_STATISTIC.get()).calculate(dependencies);

            return ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                    CalculatedValue.Builder.class,
                    b -> b.setValue(sum.getValue().divide(count.getValue())));
        }
    }
}
