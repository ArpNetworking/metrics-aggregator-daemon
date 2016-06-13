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
import com.arpnetworking.tsdcore.model.Quantity;

import java.util.List;
import java.util.Set;

/**
 * The aggregation is performed with a user-defined expression. At this time
 * this class is only a place holder, but evaluation of the expression will
 * in theory be moved into each unique instance in the future. Use
 * <code>StatisticFactory</code> for construction.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class ExpressionStatistic extends BaseStatistic {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "expression";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Accumulator<Void> createCalculator() {
        throw new UnsupportedOperationException("Calculation of ExpressionStatistic instances is not supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Statistic> getDependencies() {
        throw new UnsupportedOperationException("Calculation of ExpressionStatistic instances is not supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculate(final List<Quantity> unorderedValues) {
        throw new UnsupportedOperationException("Calculation of ExpressionStatistic instances is not supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        throw new UnsupportedOperationException("Calculation of ExpressionStatistic instances is not supported");
    }

    private ExpressionStatistic() { }

    private static final long serialVersionUID = -9159667444288515901L;
}
