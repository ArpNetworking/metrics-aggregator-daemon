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
package com.arpnetworking.metrics.mad.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of the <code>Record</code> interface.
 *
 * @author Brandon Arp (barp at groupon dot com)
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@Loggable
public final class DefaultRecord implements Record {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return _id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DateTime getTime() {
        return _time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHost() {
        return _host;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getService() {
        return _service;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCluster() {
        return _cluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ? extends Metric> getMetrics() {
        return _metrics;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAnnotations() {
        return _annotations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof Record)) {
            return false;
        }

        final Record otherRecord = (Record) other;
        return Objects.equal(getId(), otherRecord.getId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Metrics", _metrics)
                .add("Id", _id)
                .add("Time", _time)
                .add("Host", _host)
                .add("Service", _service)
                .add("Cluster", _cluster)
                .add("Annotations", _annotations)
                .toString();
    }

    // NOTE: Invoked through reflection by OvalBuilder
    @SuppressWarnings("unused")
    private DefaultRecord(final Builder builder) {
        _metrics = ImmutableMap.copyOf(builder._metrics);
        _id = builder._id;
        _time = builder._time;
        _host = builder._host;
        _service = builder._service;
        _cluster = builder._cluster;
        _annotations = ImmutableMap.copyOf(builder._annotations);
    }

    private final ImmutableMap<String, ? extends Metric> _metrics;
    private final String _id;
    private final DateTime _time;
    private final String _host;
    private final String _service;
    private final String _cluster;
    private final ImmutableMap<String, String> _annotations;

    /**
     * Implementation of builder pattern for <code>DefaultRecord</code>.
     *
     * @author Ville Koskela (vkoskela at groupon dot com)
     */
    public static final class Builder extends OvalBuilder<Record> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DefaultRecord::new);
        }

        /**
         * The named metrics <code>Map</code>. Cannot be null.
         *
         * @param value The named metrics <code>Map</code>.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMetrics(final Map<String, ? extends Metric> value) {
            _metrics = value;
            return this;
        }

        /**
         * The identifier of the record. Cannot be null or empty.
         *
         * @param value The identifier.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setId(final String value) {
            _id = value;
            return this;
        }

        /**
         * The timestamp of the record. Cannot be null.
         *
         * @param value The timestamp.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setTime(final DateTime value) {
            _time = value;
            return this;
        }

        /**
         * The host of the record. Cannot be null or empty.
         *
         * @param value The host.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setHost(final String value) {
            _host = value;
            return this;
        }

        /**
         * The service of the record. Cannot be null or empty.
         *
         * @param value The service.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setService(final String value) {
            _service = value;
            return this;
        }

        /**
         * The cluster of the record. Cannot be null or empty.
         *
         * @param value The cluster.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setCluster(final String value) {
            _cluster = value;
            return this;
        }

        /**
         * The annotations <code>Map</code>. Optional. Default is an empty
         * <code>Map</code>. Cannot be null.
         *
         * @param value The annotations <code>Map</code>.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setAnnotations(final Map<String, String> value) {
            _annotations = value;
            return this;
        }

        @NotNull
        private Map<String, ? extends Metric> _metrics;
        @NotNull
        @NotEmpty
        private String _id;
        @NotNull
        private DateTime _time;
        @NotNull
        @NotEmpty
        private String _host;
        @NotNull
        @NotEmpty
        private String _service;
        @NotNull
        @NotEmpty
        private String _cluster;
        @NotNull
        private Map<String, String> _annotations = Collections.emptyMap();
    }
}
