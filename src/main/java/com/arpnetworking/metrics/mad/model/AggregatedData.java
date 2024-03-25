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
package com.arpnetworking.metrics.mad.model;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.base.Objects;
import net.sf.oval.constraint.NotNull;

import javax.annotation.Nullable;

/**
 * Representation of aggregated data; also known as a statistic. However, not
 * to be confused with the identity of a statistic
 * ({@link com.arpnetworking.metrics.mad.model.statistics.Statistic}). This
 * class represents output from aggregation, but also input to aggregation
 * when clients have performed pre-aggregation.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class AggregatedData {

    public Statistic getStatistic() {
        return _statistic;
    }

    public boolean getIsSpecified() {
        return _isSpecified;
    }

    public Quantity getValue() {
        return _value;
    }

    public long getPopulationSize() {
        return _populationSize;
    }

    public Object getSupportingData() {
        return _supportingData;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        final AggregatedData other = (AggregatedData) object;

        return Objects.equal(_value, other._value)
                && Objects.equal(_statistic, other._statistic)
                && Long.compare(_populationSize, other._populationSize) == 0
                && Objects.equal(_isSpecified, other._isSpecified)
                && Objects.equal(_supportingData, other._supportingData);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                getStatistic(),
                getValue(),
                getPopulationSize(),
                getIsSpecified(),
                getSupportingData());
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * NOTE: This class is not marked @Loggable due to the potentially large
     * number of samples in the _samples field.  Using @Loggable would cause them
     * all to be serialized and in the past has caused significant performance
     * problems.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("statistic", _statistic)
                .put("value", _value)
                .put("populationSize", _populationSize)
                .put("getIsSpecified", _isSpecified)
                .build();
    }

    private AggregatedData(final Builder builder) {
        _statistic = builder._statistic;
        _value = builder._value;
        _populationSize = builder._populationSize;
        _isSpecified = builder._isSpecified;
        _supportingData = builder._supportingData;
    }

    private final Statistic _statistic;
    private final Quantity _value;
    private final long _populationSize;
    private final boolean _isSpecified;
    private final Object _supportingData;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link AggregatedData}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends ThreadLocalBuilder<AggregatedData> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregatedData::new);
        }

        /**
         * The statistic. Required. Cannot be null.
         *
         * @param value The {@link Statistic}.
         * @return This instance of {@link Builder}.
         */
        public Builder setStatistic(final Statistic value) {
            _statistic = value;
            return this;
        }

        /**
         * The value. Required. Cannot be null.
         *
         * @param value The value.
         * @return This instance of {@link Builder}.
         */
        public Builder setValue(final Quantity value) {
            _value = value;
            return this;
        }

        /**
         * The population size. Required. Cannot be null.
         *
         * @param value The samples.
         * @return This instance of {@link Builder}.
         */
        public Builder setPopulationSize(final Long value) {
            _populationSize = value;
            return this;
        }

        /**
         * The aggregated data was specified. Required. Cannot be null.
         *
         * @param value The metric type.
         * @return This instance of {@link Builder}.
         */
        public Builder setIsSpecified(final Boolean value) {
            _isSpecified = value;
            return this;
        }

        /**
         * The supporting data.
         *
         * @param value The supporting data.
         * @return This instance of {@link Builder}.
         */
        public Builder setSupportingData(@Nullable final Object value) {
            _supportingData = value;
            return this;
        }

        @Override
        public AggregatedData build() {
            if (_statistic == null) {
                throw new IllegalStateException("statistic must not be null");
            }
            if (_value == null) {
                throw new IllegalStateException("value must not be null");
            }
            if (_populationSize == null) {
                throw new IllegalStateException("populationSize must not be null");
            }
            if (_isSpecified == null) {
                throw new IllegalStateException("getIsSpecified must not be null");
            }
            return new AggregatedData(this);
        }

        @Override
        protected void reset() {
            _statistic = null;
            _value = null;
            _populationSize = null;
            _isSpecified = null;
            _supportingData = null;
        }

        @NotNull
        private Statistic _statistic;
        @NotNull
        private Quantity _value;
        @NotNull
        private Long _populationSize;
        @NotNull
        private Boolean _isSpecified;
        private Object _supportingData;
    }
}
