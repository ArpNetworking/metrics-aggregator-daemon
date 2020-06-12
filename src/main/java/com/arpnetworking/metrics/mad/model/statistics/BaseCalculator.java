/*
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
package com.arpnetworking.metrics.mad.model.statistics;

/**
 * A calculator base class.
 *
 * @param <T> The type of the supporting data produced by the {@link Calculator}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public abstract class BaseCalculator<T> implements Calculator<T> {

    @Override
    public Statistic getStatistic() {
        return _statistic;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        return this == o || (o != null && getClass().equals(o.getClass()));
    }

    /**
     * Protected constructor.
     *
     * @param statistic The {@link Statistic} this {@link Calculator} is for.
     */
    protected BaseCalculator(final Statistic statistic) {
        _statistic = statistic;
    }

    private final Statistic _statistic;
}
