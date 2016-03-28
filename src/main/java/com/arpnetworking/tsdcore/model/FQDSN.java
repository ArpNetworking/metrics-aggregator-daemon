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
import com.google.common.base.Strings;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;

/**
 * Fully qualified data space name. This uniquely describes the space of all
 * data for a metric in the system. Effectively, this is a hypercube divided
 * by several dimensions including time, period and host as well as any user
 * specified dimensions.
 *
 * The FQDSN is composed of the cluster name, service name, metric name, and
 * statistic. To refer to a specific value in the data space or hypercube of
 * a metric you should use a fully qualified statistic name (<code>FQSN</code>).
 *
 * The identified data space is a time series by period.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@Loggable
public final class FQDSN implements Serializable {

    public String getCluster() {
        return _cluster;
    }

    public String getService() {
        return _service;
    }

    public String getMetric() {
        return _metric;
    }

    public Statistic getStatistic() {
        return _statistic;
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

        final FQDSN other = (FQDSN) object;

        return Objects.equal(_metric, other._metric)
                && Objects.equal(_service, other._service)
                && Objects.equal(_cluster, other._cluster)
                && Objects.equal(_statistic, other._statistic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(
                getCluster(),
                getService(),
                getMetric(),
                getStatistic());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Cluster", _cluster)
                .add("Service", _service)
                .add("Metric", _metric)
                .add("Statistic", _statistic)
                .toString();
    }

    private FQDSN(final Builder builder) {
        _cluster = builder._cluster;
        _service = builder._service;
        _metric = builder._metric;
        _statistic = builder._statistic;
    }

    private final String _cluster;
    private final String _service;
    private final String _metric;
    private final Statistic _statistic;

    private static final long serialVersionUID = 3250912975329607150L;

    /**
     * Implementation of builder pattern for <code>FQDSN</code>.
     *
     * @author Ville Koskela (vkoskela at groupon dot com)
     */
    public static final class Builder extends OvalBuilder<FQDSN> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(FQDSN::new);
        }

        /**
         * The cluster. Cannot be null or empty.
         *
         * @param value The cluster.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setCluster(final String  value) {
            _cluster = value;
            return this;
        }

        /**
         * The service. Cannot be null or empty.
         *
         * @param value The service.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setService(final String value) {
            _service = value;
            return this;
        }

        /**
         * The metric. Cannot be null or empty.
         *
         * @param value The metric.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMetric(final String value) {
            _metric = value;
            return this;
        }

        /**
         * The statistic. Cannot be null.
         *
         * @param value The statistic.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setStatistic(final Statistic value) {
            _statistic = value;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FQDSN build() {
            if (Strings.isNullOrEmpty(_cluster)) {
                throw new IllegalStateException("cluster must not be null or empty");
            }
            if (Strings.isNullOrEmpty(_service)) {
                throw new IllegalStateException("service must not be null or empty");
            }
            if (Strings.isNullOrEmpty(_metric)) {
                throw new IllegalStateException("metric must not be null or empty");
            }
            if (_statistic == null) {
                throw new IllegalStateException("statistic must not be null");
            }
            return new FQDSN(this);
        }

        @NotNull
        @NotEmpty
        private String _cluster;
        @NotNull
        @NotEmpty
        private String _service;
        @NotNull
        @NotEmpty
        private String _metric;
        @NotNull
        private Statistic _statistic;
    }
}
