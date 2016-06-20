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

import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Quantity;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Interface for a statistic calculator.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public interface Statistic extends Serializable {

    /**
     * Accessor for the name of the statistic.
     *
     * @return The name of the statistic.
     */
    String getName();

    /**
     * Accessor for any aliases of the statistic.
     *
     * @return The aliases of the statistic.
     */
    Set<String> getAliases();

    /**
     * Create a <code>Calculator</code> for this statistic.
     *
     * @return The new <code>Calculator</code> instance.
     */
    Calculator<?> createCalculator();

    /**
     * Accessor for any dependencies.
     *
     * @return The <code>Set</code> of <code>Statistic</code> dependencies.
     */
    Set<Statistic> getDependencies();

    /**
     * Compute the statistic from the <code>list</code> of <code>Quantity</code>
     * instances. By default the <code>List</code> of samples is not assumed to
     * be in any particular order. However, any <code>Statistic</code> subclass
     * may implement the marker interface <code>OrderedStatistic</code>
     * indicating a requirement to be provided with samples that are sorted from
     * smallest to largest. In all cases the samples are required to be unified
     * into the same unit (or no unit).
     *
     * @param values <code>List</code> of samples <code>Quantity</code> instances.
     * @return Computed statistic <code>Quantity</code> instance.
     */
    Quantity calculate(List<Quantity> values);

    /**
     * Compute the statistic from the <code>List</code> of <code>AggregatedData</code>
     * instances. By default the <code>List</code> of samples is not assumed to be in
     * any particular order. However, any <code>Statistic</code> subclass may implement
     * the marker interface <code>OrderedStatistic</code> indicating a requirement to
     * be provided with samples that are sorted from smallest to largest. In all cases
     * the samples are required to be unified into the same unit (or no unit).
     *
     * @param aggregations Aggregations to combine.
     * @return Computed statistic value.
     */
    Quantity calculateAggregations(List<AggregatedData> aggregations);
}
