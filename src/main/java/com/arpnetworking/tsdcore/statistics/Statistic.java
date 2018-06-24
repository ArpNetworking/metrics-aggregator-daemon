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
package com.arpnetworking.tsdcore.statistics;

import java.io.Serializable;
import java.util.Set;

/**
 * Interface for a statistic calculator.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
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
}
