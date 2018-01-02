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
package com.arpnetworking.metrics.mad.model.json;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Model for the version 2c query log line.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class Version2c {

    public Map<String, List<String>> getTimers() {
        return _timers;
    }

    public Map<String, List<String>> getGauges() {
        return _gauges;
    }

    public Map<String, List<String>> getCounters() {
        return _counters;
    }

    public Annotations getAnnotations() {
        return _annotations;
    }

    public String getVersion() {
        return _version;
    }

    private Version2c(final Builder builder) {
        _annotations = builder._annotations;
        _version = builder._version;
        _timers = ImmutableMap.copyOf(builder._timers);
        _gauges = ImmutableMap.copyOf(builder._gauges);
        _counters = ImmutableMap.copyOf(builder._counters);
    }

    private final Annotations _annotations;
    private final ImmutableMap<String, List<String>> _counters;
    private final ImmutableMap<String, List<String>> _timers;
    private final ImmutableMap<String, List<String>> _gauges;
    private final String _version;

    /**
     * Builder for the Version2c class.
     */
    public static final class Builder extends ThreadLocalBuilder<Version2c> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Version2c::new);
        }

        /**
         * Sets the annotations field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setAnnotations(final Annotations value) {
            _annotations = value;
            return this;
        }

        /**
         * Sets the version field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setVersion(final String value) {
            _version = value;
            return this;
        }

        /**
         * Sets the counters field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setCounters(final Map<String, List<String>> value) {
            _counters = value;
            return this;
        }

        /**
         * Sets the timers field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setTimers(final Map<String, List<String>> value) {
            _timers = value;
            return this;
        }

        /**
         * Sets the gauges field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setGauges(final Map<String, List<String>> value) {
            _gauges = value;
            return this;
        }

        @Override
        protected void reset() {
            _annotations = null;
            _counters = Collections.emptyMap();
            _gauges = Collections.emptyMap();
            _timers = Collections.emptyMap();
            _version = null;
        }

        @NotNull
        private Annotations _annotations;
        @NotNull
        private Map<String, List<String>> _counters = Collections.emptyMap();
        @NotNull
        private Map<String, List<String>> _gauges = Collections.emptyMap();
        @NotNull
        private Map<String, List<String>> _timers = Collections.emptyMap();
        @NotNull
        @MatchPattern(pattern = "^2c$", flags = Pattern.CASE_INSENSITIVE)
        private String _version;
    }

    /**
     * Represents the set of annotations on a line.
     */
    @Loggable
    public static final class Annotations {
        public Optional<String> getInitTimestamp() {
            return _initTimestamp;
        }

        public Optional<String> getFinalTimestamp() {
            return _finalTimestamp;
        }

        public Map<String, String> getOtherAnnotations() {
            return _otherAnnotations;
        }

        private Annotations(final Annotations.Builder builder) {
            _initTimestamp = Optional.ofNullable(builder._initTimestamp);
            _finalTimestamp = Optional.ofNullable(builder._finalTimestamp);
            _otherAnnotations = ImmutableMap.copyOf(builder._otherAnnotations);
        }

        private final Optional<String> _initTimestamp;
        private final Optional<String> _finalTimestamp;
        private final ImmutableMap<String, String> _otherAnnotations;

        /**
         * Builder for the Annotations class.
         */
        public static final class Builder extends ThreadLocalBuilder<Annotations> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Annotations::new);
            }

            /**
             * Sets the initTimestamp field.
             *
             * @param value Value
             * @return This builder
             */
            public Annotations.Builder setInitTimestamp(final String value) {
                _initTimestamp = value;
                return this;
            }

            /**
             * Sets the finalTimestamp field.
             *
             * @param value Value
             * @return This builder
             */
            public Annotations.Builder setFinalTimestamp(final String value) {
                _finalTimestamp = value;
                return this;
            }

            /**
             * Called by json deserialization to store non-member elements of
             * the json object. Stores the value in the otherAnnotations field.
             *
             * @param key key
             * @param value value
             */
            @JsonAnySetter
            public void handleUnknown(final String key, final Object value) {
                if (value instanceof String) {
                    _otherAnnotations.put(key, (String) value);
                }
            }

            @Override
            protected void reset() {
                _finalTimestamp = null;
                _initTimestamp = null;
                _otherAnnotations = Maps.newHashMap();
            }

            private String _finalTimestamp;
            private String _initTimestamp;
            @NotNull
            private Map<String, String> _otherAnnotations = Maps.newHashMap();
        }
    }
}
