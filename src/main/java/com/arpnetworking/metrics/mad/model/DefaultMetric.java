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
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotNull;

import java.util.List;

/**
 * A variable and data to describe the input to a statistic calculator.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class DefaultMetric implements Metric {

    @Override
    public MetricType getType() {
        return _type;
    }

    @Override
    public List<Quantity> getValues() {
        return _values;
    }

    @Override
    public ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> getStatistics() {
        return _statistics;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof Metric)) {
            return false;
        }

        final Metric otherMetric = (Metric) other;
        return Objects.equal(getType(), otherMetric.getType())
                && Objects.equal(getValues(), otherMetric.getValues())
                && Objects.equal(getStatistics(), otherMetric.getStatistics());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getType(), getValues(), getStatistics());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Type", _type)
                .add("Values", _values)
                .add("Statistics", _statistics)
                .toString();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * NOTE: This class is not marked @Loggable due to the potentially large
     * number of samples in the value field.  Using @Loggable would cause them
     * all to be serialized and in the past has caused significant performance
     * problems.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("type", _type)
                .put("valueSize", _values.size())
                .put("statisticsSize", _statistics.size())
                .build();
    }

    private DefaultMetric(final Builder builder) {
        _type = builder._type;
        _values = builder._values;
        _statistics = builder._statistics;
    }

    private final MetricType _type;
    private final ImmutableList<Quantity> _values;
    private final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> _statistics;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link DefaultMetric}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends ThreadLocalBuilder<Metric> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DefaultMetric::new);
        }

        /**
         * The statistics {@code Map}. Cannot be null.
         *
         * @param value The values {@code List}.
         * @return This instance of {@link Builder}.
         */
        public Builder setStatistics(final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> value) {
            _statistics = value;
            return this;
        }

        /**
         * The values {@code List}. Cannot be null.
         *
         * @param value The values {@code List}.
         * @return This instance of {@link Builder}.
         */
        public Builder setValues(final ImmutableList<Quantity> value) {
            _values = value;
            return this;
        }

        /**
         * The metric type. Cannot be null.
         *
         * @param value The metric type.
         * @return This instance of {@link Builder}.
         */
        public Builder setType(final MetricType value) {
            _type = value;
            return this;
        }

        @Override
        protected void reset() {
            _statistics = ImmutableMap.of();
            _values = ImmutableList.of();
            _type = null;
        }

        @NotNull
        private ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> _statistics = ImmutableMap.of();
        @NotNull
        private ImmutableList<Quantity> _values = ImmutableList.of();
        @NotNull
        private MetricType _type;
    }
}
