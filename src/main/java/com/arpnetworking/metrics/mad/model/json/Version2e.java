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

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.tsdcore.model.Unit;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Model for the version 2e query log line.
 *
 * Notes:
 * <ul>
 * <li>For optional fields null and unspecified are treated the same. Where
 * appropriate this means null is mapped to a default value (e.g. empty map) or
 * wrapped in an <code>Optional</code>.</li>
 * <li>Although Steno specifies the <code>data</code> element is optional, it is
 * required for consumption by Tsd Aggregator.</li>
 * </ul>
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Loggable
public final class Version2e {

    public DateTime getTime() {
        return _time;
    }

    public String getName() {
        return _name;
    }

    public String getLevel() {
        return _level;
    }

    public Data getData() {
        return _data;
    }

    public Optional<String> getId() {
        return _id;
    }

    public Map<String, String> getContext() {
        return _context;
    }

    private Version2e(final Builder builder) {
        _time = builder._time;
        _name = builder._name;
        _level = builder._level;
        _data = builder._data;
        _id = Optional.ofNullable(builder._id);
        _context = ImmutableMap.copyOf(MoreObjects.firstNonNull(builder._context, Collections.<String, String>emptyMap()));
    }

    private final DateTime _time;
    private final String _name;
    private final String _level;
    private final Data _data;
    private final Optional<String> _id;
    private final ImmutableMap<String, String> _context;

    /**
     * Builder for the Version2e class.
     */
    public static final class Builder extends OvalBuilder<Version2e> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(Version2e::new);
        }

        /**
         * Sets the time field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setTime(final DateTime value) {
            _time = value;
            return this;
        }

        /**
         * Sets the name field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setName(final String value) {
            _name = value;
            return this;
        }

        /**
         * Sets the level field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setLevel(final String value) {
            _level = value;
            return this;
        }

        /**
         * Sets the data field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setData(final Version2e.Data value) {
            _data = value;
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
         * Sets the context field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setContext(final Map<String, String> value) {
            _context = value;
            return this;
        }

        @NotNull
        private DateTime _time;
        @NotNull
        @MatchPattern(pattern = "^aint\\.metrics$")
        private String _name;
        @NotNull
        @MatchPattern(pattern = "^(debug|info|warn|crit|fatal|unknown)$")
        private String _level;
        @NotNull
        private Data _data;
        private String _id;
        private Map<String, String> _context;
    }

    /**
     * Represents the metrics data or payload in the container.
     */
    @Loggable
    public static final class Data {

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

        private Data(final Builder builder) {
            _annotations = builder._annotations;
            _version = builder._version;
            _timers = ImmutableMap.copyOf(MoreObjects.firstNonNull(builder._timers, Collections.<String, Element>emptyMap()));
            _gauges = ImmutableMap.copyOf(MoreObjects.firstNonNull(builder._gauges, Collections.<String, Element>emptyMap()));
            _counters = ImmutableMap.copyOf(MoreObjects.firstNonNull(builder._counters, Collections.<String, Element>emptyMap()));
        }

        private final Annotations _annotations;
        private final ImmutableMap<String, Element> _counters;
        private final ImmutableMap<String, Element> _timers;
        private final ImmutableMap<String, Element> _gauges;
        private final String _version;

        /**
         * Builder for the Data class.
         */
        public static final class Builder extends OvalBuilder<Data> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(Data::new);
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
            private Map<String, Element> _counters;
            private Map<String, Element> _gauges;
            private Map<String, Element> _timers;
            @NotNull
            @MatchPattern(pattern = "^2e$", flags = Pattern.CASE_INSENSITIVE)
            private String _version;
        }
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
        public static final class Builder extends OvalBuilder<Sample> {
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

        public ImmutableMap<String, String> getOtherAnnotations() {
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
                super(Annotations::new);
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
