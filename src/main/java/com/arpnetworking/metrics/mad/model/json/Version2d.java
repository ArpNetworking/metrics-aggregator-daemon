/**
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

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.tsdcore.model.Unit;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotNull;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Model for the version 2d query log line.
 *
 * @author Brandon Arp (barp at groupon dot com)
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
    public static final class Builder extends OvalBuilder<Version2d> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Version2d.class);
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
            _unit = Optional.fromNullable(builder._unit);
            _value = builder._value;
        }

        private final Optional<Unit> _unit;
        private final double _value;

        /**
         * Builder for the Sample class.
         */
        public static final class Builder extends OvalBuilder<Sample> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Sample.class);
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
        public static final class Builder extends OvalBuilder<Element> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Element.class);
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

            @NotNull
            private List<Sample> _values = Collections.emptyList();
        }
    }

    /**
     * Represents the set of annotations on a line.
     */
    @Loggable
    public static final class Annotations {
        public DateTime getInitTimestamp() {
            return _initTimestamp;
        }

        public DateTime getFinalTimestamp() {
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

        private final DateTime _finalTimestamp;
        private final DateTime _initTimestamp;
        private final ImmutableMap<String, String> _otherAnnotations;

        /**
         * Builder for the Annotations class.
         */
        public static final class Builder extends OvalBuilder<Annotations> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Annotations.class);
            }

            /**
             * Sets the initTimestamp field.
             *
             * @param value Value
             * @return This builder
             */
            public Annotations.Builder setInitTimestamp(final DateTime value) {
                _initTimestamp = value;
                return this;
            }

            /**
             * Sets the finalTimestamp field.
             *
             * @param value Value
             * @return This builder
             */
            public Annotations.Builder setFinalTimestamp(final DateTime value) {
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

            @NotNull
            private DateTime _finalTimestamp;
            @NotNull
            private DateTime _initTimestamp;
            @NotNull
            private final Map<String, String> _otherAnnotations = Maps.newHashMap();
        }
    }
}
