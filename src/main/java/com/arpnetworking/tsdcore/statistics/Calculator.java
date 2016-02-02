/**
 * Copyright 2015 Groupon.com
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

import com.arpnetworking.tsdcore.model.CalculatedValue;

import java.util.Map;

/**
 * Interface for classes providing computation of a statistic.
 *
 * @param <T> The type of supporting data.
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public interface Calculator<T> {

    /**
     * Accessor for the <code>Statistic</code> computed by this <code>Calculator</code>.
     *
     * @return The <code>Statistic</code> computed by this <code>Calculator</code>.
     */
    Statistic getStatistic();

    /**
     * Compute the value of a statistic.
     *
     * @param dependencies The <code>Map</code> of <code>Statistic</code> to its <code>Calculator</code>.
     * @return The <code>CalculatedValue</code> for the statistic.
     */
    CalculatedValue<T> calculate(Map<Statistic, Calculator<?>> dependencies);
}
