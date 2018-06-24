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

import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A statistic base class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public abstract class BaseStatistic implements Statistic {

    @Override
    public Set<String> getAliases() {
        return Collections.emptySet();
    }

    @Override
    public Set<Statistic> getDependencies() {
        return Collections.emptySet();
    }

    @Override
    public int hashCode() {
        return _hashCodeSupplier.get();
    }

    @Override
    public boolean equals(final Object o) {
        return this == o || (o != null && getClass().equals(o.getClass()));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("name", getName())
                .add("aliases", getAliases())
                .toString();
    }

    /**
     * Assert that the new unit is compatible with the existing unit.
     *
     * @param newUnit the new unit under consideration
     * @param existingUnit the unit encountered previously
     * @param hasData whether data has been processed
     */
    protected static void assertUnit(
            final Optional<Unit> newUnit,
            final Optional<Unit> existingUnit,
            final boolean hasData) {
        // If there's no data then the new unit is compatible regardless of what it is
        if (!hasData) {
            return;
        }
        // Otherwise the two units need to be the same
        if (!Objects.equals(newUnit, existingUnit)) {
            throw new IllegalStateException(String.format(
                    "Units must both be the same; newUnit=%s, existingUnit=%s",
                    newUnit,
                    existingUnit));
        }
    }

    private final Supplier<Integer> _hashCodeSupplier = Suppliers.memoize(() -> getClass().hashCode());

    private static final long serialVersionUID = -1334453626232464982L;
}
