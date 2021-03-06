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
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.time.ZonedDateTime;
import java.util.Optional;
import javax.annotation.Nullable;


/**
 * Default implementation of the {@link Record} interface.
 *
 * <b>IMPORTANT:</b> Instances are only hashed and compare based on their
 * identifier, and are considered interchangeable if their identifiers match
 * irregardless of their content.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
@Loggable
public final class DefaultRecord implements Record {

    @Override
    public String getId() {
        return _id;
    }

    @Override
    public ZonedDateTime getTime() {
        return _time;
    }

    @Override
    public Optional<ZonedDateTime> getRequestTime() {
        return _requestTime;
    }

    @Override
    public ImmutableMap<String, ? extends Metric> getMetrics() {
        return _metrics;
    }

    @Override
    public ImmutableMap<String, String> getAnnotations() {
        return _annotations;
    }

    @Override
    public ImmutableMap<String, String> getDimensions() {
        return _dimensions;
    }

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

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Metrics", _metrics)
                .add("Id", _id)
                .add("Time", _time)
                .add("Annotations", _annotations)
                .add("Dimensions", _dimensions)
                .toString();
    }

    // NOTE: Invoked through reflection by OvalBuilder
    @SuppressWarnings("unused")
    private DefaultRecord(final Builder builder) {
        _metrics = builder._metrics;
        _id = builder._id;
        _time = builder._time;
        _requestTime = Optional.ofNullable(builder._requestTime);
        _annotations = builder._annotations;
        _dimensions = builder._dimensions;
    }

    private final ImmutableMap<String, ? extends Metric> _metrics;
    private final String _id;
    private final ZonedDateTime _time;
    private final Optional<ZonedDateTime> _requestTime;
    private final ImmutableMap<String, String> _annotations;
    private final ImmutableMap<String, String> _dimensions;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link DefaultRecord}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends ThreadLocalBuilder<Record> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DefaultRecord::new);
        }

        /**
         * The named metrics {@link ImmutableMap}. Cannot be null.
         *
         * @param value The named metrics {@link ImmutableMap}.
         * @return This instance of {@link Builder}.
         */
        public Builder setMetrics(final ImmutableMap<String, ? extends Metric> value) {
            _metrics = value;
            return this;
        }

        /**
         * The identifier of the record. Cannot be null or empty.
         *
         * @param value The identifier.
         * @return This instance of {@link Builder}.
         */
        public Builder setId(final String value) {
            _id = value;
            return this;
        }

        /**
         * The timestamp of the record. Cannot be null.
         *
         * @param value The timestamp.
         * @return This instance of {@link Builder}.
         */
        public Builder setTime(final ZonedDateTime value) {
            _time = value;
            return this;
        }

        /**
         * The timestamp at which the record was received. Can be null.
         *
         * @param value The timestamp.
         * @return This instance of {@link Builder}.
         */
        public Builder setRequestTime(final ZonedDateTime value) {
            _requestTime = value;
            return this;
        }

        /**
         * The annotations {@link ImmutableMap}. Optional. Default is an empty
         * {@link ImmutableMap}. Cannot be null.
         *
         * @param value The annotations {@link ImmutableMap}.
         * @return This instance of {@link Builder}.
         */
        public Builder setAnnotations(final ImmutableMap<String, String> value) {
            _annotations = value;
            return this;
        }

        /**
         * The dimension mappings {@link ImmutableMap}. Optional. Default is an empty
         * {@link ImmutableMap}. Cannot be null.
         *
         * @param value The dimension mappings {@link ImmutableMap}
         * @return This instance of {@link Builder}.
         */
        public Builder setDimensions(final ImmutableMap<String, String> value) {
            _dimensions = value;
            return this;
        }

        @Override
        protected void reset() {
            _metrics = null;
            _id = null;
            _time = null;
            _annotations = ImmutableMap.of();
            _dimensions = ImmutableMap.of();
            _requestTime = null;
        }

        @NotNull
        private ImmutableMap<String, ? extends Metric> _metrics;
        @NotNull
        @NotEmpty
        private String _id;
        @NotNull
        private ZonedDateTime _time;
        @Nullable
        private ZonedDateTime _requestTime;
        @NotNull
        private ImmutableMap<String, String> _annotations = ImmutableMap.of();
        @NotNull
        private ImmutableMap<String, String> _dimensions = ImmutableMap.of();
    }
}
