/*
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

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMultimap;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Contains the data for a specific period in time.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@Loggable
public final class PeriodicData {

    public Duration getPeriod() {
        return _period;
    }

    public ZonedDateTime getStart() {
        return _start;
    }

    public Optional<ZonedDateTime> getMinRequestTime() {
        return _minRequestTime;
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
                && Objects.equal(_start, other._start)
                && Objects.equal(_minRequestTime, other._minRequestTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                _data,
                _dimensions,
                _period,
                _start,
                _minRequestTime);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Period", _period)
                .add("Start", _start)
                .add("Dimensions", _dimensions)
                .add("Data", _data)
                .add("MinRequestTime", _minRequestTime)
                .toString();
    }

    private PeriodicData(final Builder builder) {
        _period = builder._period;
        _start = builder._start;
        _minRequestTime = Optional.ofNullable(builder._minRequestTime);
        _dimensions = builder._dimensions;
        _data = builder._data;
    }

    private final Duration _period;
    private final ZonedDateTime _start;
    private final Optional<ZonedDateTime> _minRequestTime;
    private final Key _dimensions;
    private final ImmutableMultimap<String, AggregatedData> _data;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link PeriodicData}.
     */
    public static final class Builder extends ThreadLocalBuilder<PeriodicData> {

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
         * @return This {@link Builder} instance.
         */
        public Builder setPeriod(final Duration value) {
            _period = value;
            return this;
        }

        /**
         * Set the start. Required. Cannot be null.
         *
         * @param value The start.
         * @return This {@link Builder} instance.
         */
        public Builder setStart(final ZonedDateTime value) {
            _start = value;
            return this;
        }

        /**
         * Set the earliest time at which data in this bucket was recorded. May be null.
         *
         * @param value The timestamp
         * @return This {@link Builder} instance.
         */
        public Builder setMinRequestTime(@Nullable final ZonedDateTime value) {
            _minRequestTime = value;
            return this;
        }


        /**
         * Set the dimensions. Required. Cannot be null.
         *
         * @param value The dimensions.
         * @return This {@link Builder} instance.
         */
        public Builder setDimensions(final Key value) {
            _dimensions = value;
            return this;
        }

        /**
         * Set the data. Optional. Cannot be null. Defaults to an empty {@link ImmutableMultimap}.
         *
         * @param value The data.
         * @return This {@link Builder} instance.
         */
        public Builder setData(final ImmutableMultimap<String, AggregatedData> value) {
            _data = value;
            return this;
        }

        @Override
        protected void reset() {
            _period = null;
            _start = null;
            _minRequestTime = null;
            _dimensions = null;
            _data = ImmutableMultimap.of();
        }

        @NotNull
        private Duration _period;
        @NotNull
        private ZonedDateTime _start;
        @Nullable
        private ZonedDateTime _minRequestTime;

        @NotNull
        private Key _dimensions;
        @NotNull
        private ImmutableMultimap<String, AggregatedData> _data = ImmutableMultimap.of();
    }
}
