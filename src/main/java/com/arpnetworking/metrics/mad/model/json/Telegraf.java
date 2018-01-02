/**
 * Copyright 2017 Inscope Metrics, Inc.
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
package com.arpnetworking.metrics.mad.model.json;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.collect.ImmutableMap;

import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;

/**
 * Model for the telegraf JSON format. As defined here:
 *
 * https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#json
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class Telegraf {

    public ImmutableMap<String, String> getFields() {
        return _fields;
    }

    public long getTimestamp() {
        return _timestamp;
    }

    public ImmutableMap<String, String> getTags() {
        return _tags;
    }

    public String getName() {
        return _name;
    }

    private Telegraf(final Builder builder) {
        _tags = builder._tags;
        _name = builder._name;
        _fields = builder._fields;
        _timestamp = builder._timestamp;
    }

    private final ImmutableMap<String, String> _tags;
    private final long _timestamp;
    private final ImmutableMap<String, String> _fields;
    private final String _name;

    /**
     * Builder for the Telegraf class.
     */
    public static final class Builder extends ThreadLocalBuilder<Telegraf> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Telegraf::new);
        }

        /**
         * Sets the tags.
         *
         * @param value the tags
         * @return This builder
         */
        public Builder setTags(final ImmutableMap<String, String> value) {
            _tags = value;
            return this;
        }

        /**
         * Sets the timestamp.
         *
         * @param value the timestamp
         * @return This builder
         */
        public Builder setTimestamp(final Long value) {
            _timestamp = value;
            return this;
        }

        /**
         * Sets the fields.
         *
         * @param value the fields
         * @return This builder
         */
        public Builder setFields(final ImmutableMap<String, String> value) {
            _fields = value;
            return this;
        }

        /**
         * Sets the name.
         *
         * @param value the name
         * @return This builder
         */
        public Builder setName(final String value) {
            _name = value;
            return this;
        }

        @Override
        protected void reset() {
            _tags = ImmutableMap.of();
            _timestamp = null;
            _fields = ImmutableMap.of();
            _name = null;
        }

        @NotNull
        private ImmutableMap<String, String> _tags = ImmutableMap.of();
        @Min(0)
        @NotNull
        private Long _timestamp;
        @NotNull
        private ImmutableMap<String, String> _fields = ImmutableMap.of();
        @NotNull
        private String _name;
    }
}
