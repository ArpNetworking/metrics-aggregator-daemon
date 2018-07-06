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
package com.inscopemetrics.metrics.mad.model;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.inscopemetrics.tsdcore.model.MetricType;
import com.inscopemetrics.tsdcore.model.Quantity;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import net.sf.oval.constraint.NotNull;

import java.util.List;

/**
 * A variable and data to describe the input to a statistic calculator.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
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
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof Metric)) {
            return false;
        }

        final Metric otherMetric = (Metric) other;
        return Objects.equal(getType(), otherMetric.getType())
                && Objects.equal(getValues(), otherMetric.getValues());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getType(), getValues());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Type", _type)
                .add("Values", _values)
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
                .build();
    }

    private DefaultMetric(final Builder builder) {
        _type = builder._type;
        _values = builder._values;
    }

    private final MetricType _type;
    private final ImmutableList<Quantity> _values;

    /**
     * Implementation of builder pattern for <code>DefaultMetric</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends ThreadLocalBuilder<Metric> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DefaultMetric::new);
        }

        /**
         * The values <code>List</code>. Cannot be null.
         *
         * @param value The values <code>List</code>.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setValues(final ImmutableList<Quantity> value) {
            _values = value;
            return this;
        }

        /**
         * The metric type. Cannot be null.
         *
         * @param value The metric type.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setType(final MetricType value) {
            _type = value;
            return this;
        }

        @Override
        protected void reset() {
            _values = null;
            _type = null;
        }

        @NotNull
        private ImmutableList<Quantity> _values;
        @NotNull
        private MetricType _type;
    }
}
