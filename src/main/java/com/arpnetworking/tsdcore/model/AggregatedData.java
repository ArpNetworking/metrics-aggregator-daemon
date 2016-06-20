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
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Serves as a data class for storing data for aggregated values after
 * computation.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public final class AggregatedData implements Serializable {

    public FQDSN getFQDSN() {
        return _fqdsn;
    }

    /**
     * @return Period.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public Period getPeriod() {
        return _period;
    }

    /**
     * @return Host.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public String getHost() {
        return _host;
    }

    /**
     * @return Period Start.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public DateTime getPeriodStart() {
        return getStart();
    }

    /**
     * @return Period Start.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public DateTime getStart() {
        return _start;
    }

    public boolean isSpecified() {
        return _isSpecified;
    }

    public Quantity getValue() {
        return _value;
    }

    public List<Quantity> getSamples() {
        return _samples;
    }

    public long getPopulationSize() {
        return _populationSize;
    }

    public Object getSupportingData() {
        return _supportingData;
    }

    /**
     * Create a fully qualified statistic name (FQSN).
     *
     * @param data The <code>AggregatedData</code> instance.
     * @return The FQSN.
     */
    public static FQSN createFQSN(final AggregatedData data) {
        // TODO(vkoskela): This is a temporary measure to aid with migrating [MAI-448]
        // away from FQSN data on the AggregatedData instance until FQSN
        // instances are plumbed throughout the codebase.
        return new FQSN.Builder()
                .fromFQDSN(data._fqdsn)
                .setPeriod(data._period)
                .setStart(data._start)
                //.addDimension("host", data._host)
                .build();
    }

    /**
     * Create a fully qualified data space name (FQDSN).
     *
     * @param data The <code>AggregatedData</code> instance.
     * @return The FQDSN.
     */
    public static FQDSN createFQDSN(final AggregatedData data) {
        // TODO(vkoskela): This is a temporary measure to aid with migrating [MAI-448]
        // away from FQDSN data on the AggregatedData instance until FQDSN
        // instances are plumbed throughout the codebase.
        return data._fqdsn;
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

        final AggregatedData other = (AggregatedData) object;

        return Objects.equal(_value, other._value)
                && Long.compare(_populationSize, other._populationSize) == 0
                && Objects.equal(_start, other._start)
                && Objects.equal(_period, other._period)
                && Objects.equal(_fqdsn, other._fqdsn)
                && Objects.equal(_host, other._host)
                && Objects.equal(_isSpecified, other._isSpecified)
                && Objects.equal(_samples, other._samples)
                && Objects.equal(_supportingData, other._supportingData);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(
                getFQDSN(),
                getHost(),
                getValue(),
                getStart(),
                getPeriod(),
                getHost(),
                getSamples(),
                getPopulationSize(),
                isSpecified(),
                getSupportingData());
    }

    /**
     * {@inheritDoc}
     */
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
                .put("fqdsn", _fqdsn)
                .put("value", _value)
                .put("samplesSize", _samples.size())
                .put("populationSize", _populationSize)
                .put("period", _period)
                .put("start", _start)
                .put("host", _host)
                .put("isSpecified", _isSpecified)
                .build();
    }

    private AggregatedData(final Builder builder) {
        _fqdsn = builder._fqdsn;
        _value = builder._value;
        if (builder._samples instanceof ImmutableList) {
            _samples = (ImmutableList<Quantity>) builder._samples;
        } else {
            _samples = ImmutableList.copyOf(builder._samples);
        }
        _populationSize = builder._populationSize;
        _period = builder._period;
        _start = builder._start;
        _host = builder._host;
        _isSpecified = builder._isSpecified;
        _supportingData = builder._supportingData;
    }

    private final FQDSN _fqdsn;
    private final Quantity _value;
    private final long _populationSize;
    private final ImmutableList<Quantity> _samples;
    private final DateTime _start;
    private final Period _period;
    private final String _host;
    private final boolean _isSpecified;
    private final Object _supportingData;

    private static final long serialVersionUID = 9124136139360447095L;

    /**
     * Implementation of builder pattern for <code>AggregatedData</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends OvalBuilder<AggregatedData> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregatedData::new);
        }

        /**
         * The fully qualified data space name (<code>FQDSN</code>). Required. Cannot be null.
         *
         * @param value The <code>FQDSN</code>.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setFQDSN(final FQDSN value) {
            _fqdsn = value;
            return this;
        }

        /**
         * The value. Required. Cannot be null.
         *
         * @param value The value.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setValue(final Quantity value) {
            _value = value;
            return this;
        }

        /**
         * The samples. Required. Cannot be null.
         *
         * @param value The samples.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setSamples(final Collection<Quantity> value) {
            _samples = value;
            return this;
        }

        /**
         * The population size. Required. Cannot be null.
         *
         * @param value The samples.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setPopulationSize(final Long value) {
            _populationSize = value;
            return this;
        }

        /**
         * The period start. Required. Cannot be null.
         *
         * @param value The period start.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setStart(final DateTime value) {
            _start = value;
            return this;
        }

        /**
         * The period. Required. Cannot be null.
         *
         * @param value The period.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setPeriod(final Period value) {
            _period = value;
            return this;
        }

        /**
         * The host. Required. Cannot be null or empty.
         *
         * @param value The host.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setHost(final String value) {
            _host = value;
            return this;
        }

        /**
         * The aggregated data was specified. Required. Cannot be null.
         *
         * @param value The metric type.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setIsSpecified(final Boolean value) {
            _isSpecified = value;
            return this;
        }

        /**
         * The supporting data.
         *
         * @param value The supporting data.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setSupportingData(final Object value) {
            _supportingData = value;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public AggregatedData build() {
            if (_fqdsn == null) {
                throw new IllegalStateException("fqdsn must not be null");
            }
            if (_value == null) {
                throw new IllegalStateException("value must not be null");
            }
            if (_samples == null) {
                throw new IllegalStateException("samples must not be null");
            }
            if (_populationSize == null) {
                throw new IllegalStateException("populationSize must not be null");
            }
            if (_start == null) {
                throw new IllegalStateException("start must not be null");
            }
            if (_period == null) {
                throw new IllegalStateException("period must not be null");
            }
            if (Strings.isNullOrEmpty(_host)) {
                throw new IllegalStateException("host must not be null or empty");
            }
            if (_isSpecified == null) {
                throw new IllegalStateException("isSpecified must not be null");
            }
            return new AggregatedData(this);
        }

        @NotNull
        private FQDSN _fqdsn;
        @NotNull
        private Quantity _value;
        @NotNull
        private Collection<Quantity> _samples = Collections.emptyList();
        @NotNull
        private Long _populationSize;
        @NotNull
        private DateTime _start;
        @NotNull
        private Period _period;
        @NotNull
        @NotEmpty
        private String _host;
        @NotNull
        private Boolean _isSpecified;
        private Object _supportingData;
    }
}
