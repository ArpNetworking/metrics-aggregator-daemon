/*
 * Copyright 2015 Groupon.com
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
import com.arpnetworking.metrics.mad.model.Unit;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Model for the version 2f query log line.
 *
 * Notes:
 * <ul>
 * <li>For optional fields null and unspecified are treated the same. Where
 * appropriate this means null is mapped to a default value (e.g. empty map) or
 * wrapped in an <code>Optional</code>.</li>
 * </ul>
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class Version2f {

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

    private Version2f(final Builder builder) {
        _annotations = builder._annotations;
        _version = builder._version;
        ImmutableMap.of();
        _timers = builder._timers == null ? ImmutableMap.of() : ImmutableMap.copyOf(builder._timers);
        _gauges = builder._gauges == null ? ImmutableMap.of() : ImmutableMap.copyOf(builder._gauges);
        _counters = builder._counters == null ? ImmutableMap.of() : ImmutableMap.copyOf(builder._counters);
    }

    private final Annotations _annotations;
    private final ImmutableMap<String, Element> _counters;
    private final ImmutableMap<String, Element> _timers;
    private final ImmutableMap<String, Element> _gauges;
    private final String _version;

    /**
     * Builder for the Data class.
     */
    public static final class Builder extends ThreadLocalBuilder<Version2f> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Version2f::new);
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
            _counters = null;
            _gauges = null;
            _timers = null;
            _version = null;
        }

        @NotNull
        private Annotations _annotations;
        private Map<String, Element> _counters;
        private Map<String, Element> _gauges;
        private Map<String, Element> _timers;
        @NotNull
        @MatchPattern(pattern = "^2f$", flags = Pattern.CASE_INSENSITIVE)
        private String _version;
    }

    /**
     * Represents a single sample.
     */
    public static final class Sample {
        public List<Unit> getUnitNumerators() {
            return _unitNumerators;
        }

        public List<Unit> getUnitDenominators() {
            return _unitDenominators;
        }

        public double getValue() {
            return _value;
        }

        private Sample(final Builder builder) {
            _unitNumerators = builder._unitNumerators == null
                    ? ImmutableList.of() : ImmutableList.copyOf(builder._unitNumerators);
            _unitDenominators = builder._unitDenominators == null
                    ? ImmutableList.of() : ImmutableList.copyOf(builder._unitDenominators);
            _value = builder._value;
        }

        private final ImmutableList<Unit> _unitNumerators;
        private final ImmutableList<Unit> _unitDenominators;
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
             * Sets the unit numerators field.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setUnitNumerators(final List<Unit> value) {
                _unitNumerators = value;
                return this;
            }

            /**
             * Sets the unit denominators field.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setUnitDenominators(final List<Unit> value) {
                _unitDenominators = value;
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
                _unitDenominators = null;
                _unitNumerators = null;
                _value = null;
            }

            private List<Unit> _unitNumerators;
            private List<Unit> _unitDenominators;
            @NotNull
            private Double _value;
        }
    }

    /**
     * Represents a counter, timer, or gauge element.  Includes a list of
     * samples.
     */
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
                _values = null;
            }

            @NotNull
            private List<Sample> _values;
        }
    }

    /**
     * Represents the set of annotations on a line.
     */
    public static final class Annotations {
        public ZonedDateTime getStart() {
            return _start;
        }

        public ZonedDateTime getEnd() {
            return _end;
        }

        public String getId() {
            return _id;
        }

        public ImmutableMap<String, String> getOtherAnnotations() {
            return _otherAnnotations;
        }

        private Annotations(final Annotations.Builder builder) {
            _start = builder._start;
            _end = builder._end;
            _id = builder._id;
            _otherAnnotations = ImmutableMap.copyOf(builder._otherAnnotations);
        }

        private final ZonedDateTime _start;
        private final ZonedDateTime _end;
        private final String _id;
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
             * Sets the start field.
             *
             * @param value Value
             * @return This builder
             */
            @JsonSetter("_start")
            public Annotations.Builder setStart(final ZonedDateTime value) {
                _start = value;
                return this;
            }

            /**
             * Sets the end field.
             *
             * @param value Value
             * @return This builder
             */
            @JsonSetter("_end")
            public Annotations.Builder setEnd(final ZonedDateTime value) {
                _end = value;
                return this;
            }

            /**
             * Sets the id field.
             *
             * @param value Value
             * @return This builder
             */
            @JsonSetter("_id")
            public Annotations.Builder setId(final String value) {
                _id = value;
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
                _start = null;
                _end = null;
                _id = null;
                _otherAnnotations = Maps.newHashMap();
            }

            @NotNull
            private ZonedDateTime _start;
            @NotNull
            private ZonedDateTime _end;
            @NotEmpty
            @NotNull
            private String _id;
            @NotNull
            private Map<String, String> _otherAnnotations = Maps.newHashMap();
        }
    }
}
