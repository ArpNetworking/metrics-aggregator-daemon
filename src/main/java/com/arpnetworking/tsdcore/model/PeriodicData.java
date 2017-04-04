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
import com.google.common.collect.ImmutableMultimap;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * Contains the data for a specific period in time.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class PeriodicData {

    public Period getPeriod() {
        return _period;
    }

    public DateTime getStart() {
        return _start;
    }

    public Key getDimensions() {
        return _dimensions;
    }

    public ImmutableMultimap<String, AggregatedData> getData() {
        return _data;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        final PeriodicData other = (PeriodicData) object;

        return Objects.equal(_data, other._data)
                && Objects.equal(_dimensions, other._dimensions)
                && Objects.equal(_period, other._period)
                && Objects.equal(_start, other._start);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                _data,
                _dimensions,
                _period,
                _start);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Period", _period)
                .add("Start", _start)
                .add("Dimensions", _dimensions)
                .add("Data", _data)
                .toString();
    }

    private PeriodicData(final Builder builder) {
        _period = builder._period;
        _start = builder._start;
        _dimensions = builder._dimensions;
        _data = builder._data;
    }

    private final Period _period;
    private final DateTime _start;
    private final Key _dimensions;
    private final ImmutableMultimap<String, AggregatedData> _data;

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
         * Set the dimensions. Required. Cannot be null.
         *
         * @param value The dimensions.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDimensions(final Key value) {
            _dimensions = value;
            return this;
        }

        /**
         * Set the data. Optional. Cannot be null. Defaults to an empty <code>ImmutableMap</code>.
         *
         * @param value The data.
         * @return This <code>Builder</code> instance.
         */
        public Builder setData(final ImmutableMultimap<String, AggregatedData> value) {
            _data = value;
            return this;
        }

        @NotNull
        private Period _period;
        @NotNull
        private DateTime _start;
        @NotNull
        private Key _dimensions;
        @NotNull
        private ImmutableMultimap<String, AggregatedData> _data = ImmutableMultimap.of();
    }
}
