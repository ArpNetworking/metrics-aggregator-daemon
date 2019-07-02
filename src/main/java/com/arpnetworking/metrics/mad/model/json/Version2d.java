/*
 * Copyright 2014 Brandon Arp
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
import com.arpnetworking.metrics.mad.model.Unit;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotNull;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Model for the version 2d query log line.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class Version2d {
    public Map<String, Element> getTimers() {
        return _timers;
    }

    public Map<String, Element> getGauges() {
        return _gauges;
    }

    public Map<String, Element> getCounters() {
        return _counters;
    }

    public Annotations getAnnotations() {
        return _annotations;
    }

    public String getVersion() {
        return _version;
    }

    private Version2d(final Builder builder) {
        _annotations = builder._annotations;
        _version = builder._version;
        _timers = ImmutableMap.copyOf(builder._timers);
        _gauges = ImmutableMap.copyOf(builder._gauges);
        _counters = ImmutableMap.copyOf(builder._counters);
    }

    private final Annotations _annotations;
    private final ImmutableMap<String, Element> _counters;
    private final ImmutableMap<String, Element> _timers;
    private final ImmutableMap<String, Element> _gauges;
    private final String _version;

    /**
     * Builder for the Version2d class.
     */
    public static final class Builder extends ThreadLocalBuilder<Version2d> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Version2d::new);
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
        public Builder setCounters(final Map<String, Element> value) {
            _counters = value;
            return this;
        }

        /**
         * Sets the timers field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setTimers(final Map<String, Element> value) {
            _timers = value;
            return this;
        }

        /**
         * Sets the gauges field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setGauges(final Map<String, Element> value) {
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
        private Map<String, Element> _counters = Collections.emptyMap();
        @NotNull
        private Map<String, Element> _gauges = Collections.emptyMap();
        @NotNull
        private Map<String, Element> _timers = Collections.emptyMap();
        @NotNull
        @MatchPattern(pattern = "^2d$", flags = Pattern.CASE_INSENSITIVE)
        private String _version;
    }

    /**
     * Represents a single sample.
     */
    @Loggable
    public static final class Sample {
        public Optional<Unit> getUnit() {
            return _unit;
        }

        public double getValue() {
            return _value;
        }

        private Sample(final Builder builder) {
            _unit = Optional.ofNullable(builder._unit);
            _value = builder._value;
        }

        private final Optional<Unit> _unit;
        private final double _value;

        /**
         * Builder for the Sample class.
         */
        public static final class Builder extends ThreadLocalBuilder<Sample> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Sample::new);
            }

            /**
             * Sets the unit field.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setUnit(final Unit value) {
                _unit = value;
                return this;
            }

            /**
             * Sets the value field.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setValue(final Double value) {
                _value = value;
                return this;
            }

            @Override
            protected void reset() {
                _unit = null;
                _value = null;
            }

            private Unit _unit;
            @NotNull
            private Double _value;
        }
    }

    /**
     * Represents a counter, timer, or gauge element.  Includes a list of
     * samples.
     */
    @Loggable
    public static final class Element {

        public List<Sample> getValues() {
            return _values;
        }

        private Element(final Element.Builder builder) {
            _values = ImmutableList.copyOf(builder._values);
        }

        private final ImmutableList<Sample> _values;

        /**
         * Builder for the Element class.
         */
        public static final class Builder extends ThreadLocalBuilder<Element> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Element::new);
            }

            /**
             * Sets the value field.
             *
             * @param value Value
             * @return This builder
             */
            public Element.Builder setValues(final List<Sample> value) {
                _values = value;
                return this;
            }

            @Override
            protected void reset() {
                _values = Collections.emptyList();
            }

            @NotNull
            private List<Sample> _values = Collections.emptyList();
        }
    }

    /**
     * Represents the set of annotations on a line.
     */
    @Loggable
    public static final class Annotations {
        public ZonedDateTime getInitTimestamp() {
            return _initTimestamp;
        }

        public ZonedDateTime getFinalTimestamp() {
            return _finalTimestamp;
        }

        public Map<String, String> getOtherAnnotations() {
            return _otherAnnotations;
        }

        private Annotations(final Annotations.Builder builder) {
            _initTimestamp = builder._initTimestamp;
            _finalTimestamp = builder._finalTimestamp;
            _otherAnnotations = ImmutableMap.copyOf(builder._otherAnnotations);
        }

        private final ZonedDateTime _finalTimestamp;
        private final ZonedDateTime _initTimestamp;
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
            public Annotations.Builder setInitTimestamp(final ZonedDateTime value) {
                _initTimestamp = value;
                return this;
            }

            /**
             * Sets the finalTimestamp field.
             *
             * @param value Value
             * @return This builder
             */
            public Annotations.Builder setFinalTimestamp(final ZonedDateTime value) {
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

            @NotNull
            private ZonedDateTime _finalTimestamp;
            @NotNull
            private ZonedDateTime _initTimestamp;
            @NotNull
            private Map<String, String> _otherAnnotations = Maps.newHashMap();
        }
    }
}
