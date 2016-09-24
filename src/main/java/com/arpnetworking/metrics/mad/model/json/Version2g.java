/**
 * Copyright 2016 Groupon.com
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
import com.arpnetworking.tsdcore.model.CompoundUnit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Model for the version 2g query log line.
 *
 * Notes:
 * <ul>
 * <li>For optional fields null and unspecified are treated the same. Where
 * appropriate this means null is mapped to a default value (e.g. empty map) or
 * wrapped in an <code>Optional</code>.</li>
 * </ul>
 *
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public final class Version2g {

    public String getId() {
        return _id;
    }

    public DateTime getTimestamp() {
        return _timestamp;
    }

    public Map<String, Element> getTimers() {
        return _timers;
    }

    public Map<String, Element> getGauges() {
        return _gauges;
    }

    public Map<String, Element> getCounters() {
        return _counters;
    }

    public Map<String, String> getAnnotations() {
        return _annotations;
    }

    public Map<String, String> getDimensions() {
        return _dimensions;
    }

    public String getVersion() {
        return _version;
    }

    private Version2g(final Builder builder) {
        _id = builder._id;
        _timestamp = builder._timestamp;
        _dimensions = builder._dimensions;
        _annotations = builder._annotations;
        _version = builder._version;
        _timers = builder._timers == null ? ImmutableMap.of() : builder._timers;
        _gauges = builder._gauges == null ? ImmutableMap.of() : builder._gauges;
        _counters = builder._counters == null ? ImmutableMap.of() : builder._counters;
    }

    private final String _id;
    private final DateTime _timestamp;
    private final ImmutableMap<String, String> _dimensions;
    private final ImmutableMap<String, String> _annotations;
    private final ImmutableMap<String, Element> _counters;
    private final ImmutableMap<String, Element> _timers;
    private final ImmutableMap<String, Element> _gauges;
    private final String _version;

    /**
     * Builder for the Data class.
     */
    public static final class Builder extends OvalBuilder<Version2g> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Version2g::new);
        }

        /**
         * Sets the date field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setTimestamp(final DateTime value) {
            _timestamp = value;
            return this;
        }

        /**
         * Sets the id field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setId(final String value) {
            _id = value;
            return this;
        }

        /**
         * Sets the annotations field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setAnnotations(final ImmutableMap<String, String> value) {
            _annotations = value;
            return this;
        }

        /**
         * Sets the dimensions field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setDimensions(final ImmutableMap<String, String> value) {
            _dimensions = value;
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
        public Builder setCounters(final ImmutableMap<String, Element> value) {
            _counters = value;
            return this;
        }

        /**
         * Sets the timers field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setTimers(final ImmutableMap<String, Element> value) {
            _timers = value;
            return this;
        }

        /**
         * Sets the gauges field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setGauges(final ImmutableMap<String, Element> value) {
            _gauges = value;
            return this;
        }

        @NotNull
        private String _id;
        @NotNull
        private DateTime _timestamp;
        @NotNull
        private ImmutableMap<String, String> _dimensions;
        @NotNull
        private ImmutableMap<String, String> _annotations;
        private ImmutableMap<String, Element> _counters;
        private ImmutableMap<String, Element> _gauges;
        private ImmutableMap<String, Element> _timers;
        @NotNull
        @MatchPattern(pattern = "^2g$", flags = Pattern.CASE_INSENSITIVE)
        private String _version;
    }

    /**
     * Represents a single sample.
     */
    public static final class Sample {

        public Unit2g getUnit2g() {
            return _unit2g;
        }

        public double getValue() {
            return _value;
        }

        private Sample(final Builder builder) {
            _unit2g = builder._unit2g;
            _value = builder._value;
        }

        private final Unit2g _unit2g;
        private final double _value;

        /**
         * Builder for the Sample class.
         */
        public static final class Builder extends OvalBuilder<Sample> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Sample::new);
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

            /**
             * Sets the compound unit field.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setUnit(final Unit2g value) {
                _unit2g = value;
                return this;
            }

            @NotNull
            private Double _value;
            private Unit2g _unit2g;
        }

        /**
         * 2G format Unit class.
         */
        public static final class Unit2g {

            public List<CompoundUnit> getNumerators() {
                return _numerators;
            }

            public List<CompoundUnit> getDenominators() {
                return _denominators;
            }

            private Unit2g(final Builder builder) {
                _numerators = builder._numerators == null
                        ? ImmutableList.of()
                        : ImmutableList.copyOf(builder._numerators);
                _denominators = builder._denominators == null
                        ? ImmutableList.of()
                        : ImmutableList.copyOf(builder._denominators);
            }

            private final List<CompoundUnit> _numerators;
            private final List<CompoundUnit> _denominators;

            /**
             * Builder for the 2G Unit class.
             */
            public static class Builder extends OvalBuilder<Unit2g> {
                /**
                 * Public constructor.
                 */
                public Builder() {
                    super(Unit2g::new);
                }
                /**
                 * Sets the unit numerators field.
                 *
                 * @param value Value
                 * @return This builder
                 */
                public Builder setNumerators(final List<CompoundUnit> value) {
                    _numerators = value;
                    return this;
                }

                /**
                 * Sets the unit denominators field.
                 *
                 * @param value Value
                 * @return This builder
                 */
                public Builder setDenominators(final List<CompoundUnit> value) {
                    _denominators = value;
                    return this;
                }

                private List<CompoundUnit> _numerators;
                private List<CompoundUnit> _denominators;
            }
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
        public static final class Builder extends OvalBuilder<Element> {
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

            @NotNull
            private List<Sample> _values;
        }
    }
}
