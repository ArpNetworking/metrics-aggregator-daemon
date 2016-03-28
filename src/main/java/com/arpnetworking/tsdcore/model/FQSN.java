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
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.Map;

/**
 * Fully qualified statistic name. This uniquely describes any statistic
 * or computed value in the system. In the hypercube that is any metric
 * the statistic is the specification of a point in each dimension and thus
 * identification of a single value. The FQSN is composed of the cluster name,
 * service name, metric name, period, period start, host and statistical
 * operator.
 *
 * In the future the FQSN will also include any user specified dimensions.
 * service name, metric name, statistic, period, period start and any
 * dimensions.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@Loggable
public final class FQSN {

    /**
     * Return the fully qualified data space name portion of the fully
     * qualified statistic name.
     *
     * @return Fully qualified data space name portion.
     */
    public FQDSN toFQDSN() {
        return _fqdsn;
    }

    public String getMetric() {
        return _fqdsn.getMetric();
    }

    public String getService() {
        return _fqdsn.getService();
    }

    public String getCluster() {
        return _fqdsn.getCluster();
    }

    public Statistic getStatistic() {
        return _fqdsn.getStatistic();
    }

    public Period getPeriod() {
        return _period;
    }

    public DateTime getStart() {
        return _start;
    }

    /*
    // TODO(vkoskela): Enable dimensions. [MAI-449]
    public Map<String, String> getDimensions() {
        return Collections.unmodifiableMap(_dimensions);
    }
    */

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

        final FQSN other = (FQSN) object;

        return Objects.equal(_fqdsn, other._fqdsn)
                && Objects.equal(_period, other._period)
                && Objects.equal(_start, other._start);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(
                _fqdsn,
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
                .add("FQDSN", _fqdsn)
                .add("Period", _period)
                .add("Start", _start)
                .add("Dimensions", _dimensions)
                .toString();
    }

    private FQSN(final Builder builder) {
        _fqdsn = builder._fqdsn.build();
        _period = builder._period;
        _start = builder._start;
        _dimensions = builder._dimensions;
    }

    private final FQDSN _fqdsn;
    private final Period _period;
    private final DateTime _start;
    private final Map<String, String> _dimensions;

    /**
     * Implementation of builder pattern for <code>FQSN</code>.
     *
     * @author Ville Koskela (vkoskela at groupon dot com)
     */
    public static final class Builder extends OvalBuilder<FQSN> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(FQSN::new);
        }

        /**
         * The full qualified data space name. This sets the cluster, service,
         * metric and statistic. Cannot be null.
         *
         * @param value The fully qualified metric name.
         * @return This instance of <code>Builder</code>.
         */
        public Builder fromFQDSN(final FQDSN value) {
            setCluster(value.getCluster());
            setService(value.getService());
            setMetric(value.getMetric());
            setStatistic(value.getStatistic());
            return this;
        }

        /**
         * The cluster. Cannot be null or empty.
         *
         * @param value The cluster.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setCluster(final String value) {
            _fqdsn.setCluster(value);
            return this;
        }

        /**
         * The service. Cannot be null or empty.
         *
         * @param value The service.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setService(final String value) {
            _fqdsn.setService(value);
            return this;
        }

        /**
         * The metric. Cannot be null or empty.
         *
         * @param value The metric.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMetric(final String value) {
            _fqdsn.setMetric(value);
            return this;
        }

        /**
         * The <code>Statistic</code> instance. Cannot be null.
         *
         * @param value The <code>Statistic</code> instance.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setStatistic(final Statistic value) {
            _fqdsn.setStatistic(value);
            return this;
        }

        /**
         * The period. Cannot be null.
         *
         * @param value The period.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setPeriod(final Period value) {
            _period = value;
            return this;
        }


        /**
         * The period start. Cannot be null.
         *
         * @param value The period start.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setStart(final DateTime value) {
            _start = value;
            return this;
        }

        /**
         * The dimensions. Optional. Cannot be null. Default is an empty
         * <code>Map</code>.
         *
         * @param value The dimensions.
         * @return This instance of <code>Builder</code>.
         */
        /*
        // TODO(vkoskela): Enable dimensions. [MAI-449]
        public Builder setDimensions(final Map<String, String> value) {
            _dimensions = Maps.newHashMap(value);
            return this;
        }
        */

        /**
         * Add dimension. Optional. Cannot be null. Default is an empty
         * <code>Map</code>.
         *
         * @param dimension The dimension.
         * @param value The value .
         * @return This instance of <code>Builder</code>.
         */
        /*
        // TODO(vkoskela): Enable dimensions. [MAI-449]
        public Builder addDimension(final String dimension, final String value) {
            if (_dimensions == null) {
                _dimensions = Maps.newHashMap();
            }
            _dimensions.put(dimension, value);
            return this;
        }
        */

        @NotNull
        private FQDSN.Builder _fqdsn = new FQDSN.Builder();
        @NotNull
        private Period _period;
        @NotNull
        private DateTime _start;
        @NotNull
        private Map<String, String> _dimensions = Maps.newHashMap();
    }
}
