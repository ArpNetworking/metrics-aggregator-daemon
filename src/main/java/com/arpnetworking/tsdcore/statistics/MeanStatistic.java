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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Takes the mean of the entries. Use <code>StatisticFactory</code> for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
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

    @Override
    public Quantity calculate(final List<Quantity> orderedValues) {
        // TODO(vkoskela): Statistic calculation should be allowed to either fail or not return a quantity. [MAI-?]
        if (orderedValues.size() == 0) {
            return ZERO;
        }
        double sum = 0;
        Optional<Unit> unit = Optional.empty();
        for (final Quantity sample : orderedValues) {
            sum += sample.getValue();
            unit = Optional.ofNullable(unit.orElse(sample.getUnit().orElse(null)));
        }
        return new Quantity.Builder().setValue(sum / orderedValues.size()).setUnit(unit.orElse(null)).build();
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        double weighted = 0D;
        int count = 0;
        Optional<Unit> unit = Optional.empty();
        for (final AggregatedData aggregation : aggregations) {
            final double populationSize = aggregation.getPopulationSize();
            weighted += aggregation.getValue().getValue() * populationSize;
            count += populationSize;
            unit = Optional.ofNullable(unit.orElse(aggregation.getValue().getUnit().orElse(null)));
        }
        return new Quantity.Builder()
                .setValue(weighted / count)
                .setUnit(unit.orElse(null))
                .build();
    }

    private MeanStatistic() { }

    private static final Quantity ZERO = new Quantity.Builder().setValue(0.0).build();
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

            return new CalculatedValue.Builder<Void>()
                    .setValue(sum.getValue().divide(count.getValue()))
                    .build();
        }
    }
}
