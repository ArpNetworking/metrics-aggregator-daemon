/**
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Contains the data for a specific period in time.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@Loggable
public final class PeriodicData {

    public Period getPeriod() {
        return _period;
    }

    public DateTime getStart() {
        return _start;
    }

    public ImmutableMap<String, String> getDimensions() {
        return _dimensions;
    }

    public ImmutableList<AggregatedData> getData() {
        return _data;
    }

    public ImmutableList<Condition> getConditions() {
        return _conditions;
    }

    /**
     * Retrieve <code>AggregatedData</code> instance by <code>FQDSN</code>.
     *
     * @param fqdsn The <code>FQDSN</code> to query for.
     * @return Tbe <code>Optional</code> instance of <code>AggregatedData</code>.
     */
    public Optional<AggregatedData> getDatumByFqdsn(final FQDSN fqdsn) {
        return Optional.fromNullable(_dataByFqdsn.get().get(fqdsn));
    }

    /**
     * Retrieve <code>Condition</code> instance by <code>FQDSN</code>.
     *
     * @param fqdsn The <code>FQDSN</code> to query for.
     * @return Tbe <code>Optional</code> instance of <code>Condition</code>.
     */
    public Optional<Condition> getConditionByFqdsn(final FQDSN fqdsn) {
        return Optional.fromNullable(_conditionsByFqdsn.get().get(fqdsn));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        final PeriodicData other = (PeriodicData) object;

        return Objects.equal(_conditions, other._conditions)
                && Objects.equal(_data, other._data)
                && Objects.equal(_dimensions, other._dimensions)
                && Objects.equal(_period, other._period)
                && Objects.equal(_start, other._start);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(
                _conditions,
                _data,
                _dimensions,
                _period,
                _start);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Period", _period)
                .add("Start", _start)
                .add("Dimensions", _dimensions)
                .add("Data", _data)
                .add("Conditions", _conditions)
                .toString();
    }

    private PeriodicData(final Builder builder) {
        _period = builder._period;
        _start = builder._start;
        _dimensions = builder._dimensions;
        _data = builder._data;
        _conditions = builder._conditions;

        _dataByFqdsn = Suppliers.memoize(
                () -> _data.stream().collect(Collectors.toMap(AggregatedData::getFQDSN, Function.identity())));
        _conditionsByFqdsn = Suppliers.memoize(
                () -> _conditions.stream().collect(Collectors.toMap(Condition::getFQDSN, Function.identity())));
    }

    private final Period _period;
    private final DateTime _start;
    private final ImmutableMap<String, String> _dimensions;
    private final ImmutableList<AggregatedData> _data;
    private final ImmutableList<Condition> _conditions;
    private final Supplier<Map<FQDSN, AggregatedData>> _dataByFqdsn;
    private final Supplier<Map<FQDSN, Condition>> _conditionsByFqdsn;

    /**
     * <code>Builder</code> implementation for <code>PeriodicData</code>.
     */
    public static final class Builder extends OvalBuilder<PeriodicData> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(PeriodicData::new);
        }

        /**
         * Set the period. Required. Cannot be null.
         *
         * @param value The period.
         * @return This <code>Builder</code> instance.
         */
        public Builder setPeriod(final Period value) {
            _period = value;
            return this;
        }

        /**
         * Set the start. Required. Cannot be null.
         *
         * @param value The start.
         * @return This <code>Builder</code> instance.
         */
        public Builder setStart(final DateTime value) {
            _start = value;
            return this;
        }

        /**
         * Set the dimensions. Optional. Cannot be null. Defaults to an empty <code>Map</code>.
         *
         * @param value The dimensions.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDimensions(final ImmutableMap<String, String> value) {
            _dimensions = value;
            return this;
        }

        /**
         * Set the data. Optional. Cannot be null. Defaults to an empty <code>List</code>.
         *
         * @param value The data.
         * @return This <code>Builder</code> instance.
         */
        public Builder setData(final ImmutableList<AggregatedData> value) {
            _data = value;
            return this;
        }

        /**
         * Set the conditions. Optional. Cannot be null. Defaults to an empty <code>List</code>.
         *
         * @param value The conditions.
         * @return This <code>Builder</code> instance.
         */
        public Builder setConditions(final ImmutableList<Condition> value) {
            _conditions = value;
            return this;
        }

        @NotNull
        private Period _period;
        @NotNull
        private DateTime _start;
        @NotNull
        private ImmutableMap<String, String> _dimensions = ImmutableMap.of();
        @NotNull
        private ImmutableList<AggregatedData> _data = ImmutableList.of();
        @NotNull
        private ImmutableList<Condition> _conditions = ImmutableList.of();
    }
}
