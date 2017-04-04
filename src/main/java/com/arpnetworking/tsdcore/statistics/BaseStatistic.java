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

import com.google.common.base.MoreObjects;

import java.util.Collections;
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
        return getClass().hashCode();
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

    private static final long serialVersionUID = -1334453626232464982L;
}
