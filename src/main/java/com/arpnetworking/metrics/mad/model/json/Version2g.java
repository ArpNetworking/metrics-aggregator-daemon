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
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.MatchPattern;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

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

    public DateTime getStart() {
        return _start;
    }

    public DateTime getEnd() {
        return _end;
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
        _start = builder._start;
        _end = builder._end;
        _dimensions = builder._dimensions;
        _annotations = builder._annotations;
        _version = builder._version;
        _timers = builder._timers == null ? ImmutableMap.of() : builder._timers;
        _gauges = builder._gauges == null ? ImmutableMap.of() : builder._gauges;
        _counters = builder._counters == null ? ImmutableMap.of() : builder._counters;
    }

    private final String _id;
    private final DateTime _start;
    private final DateTime _end;
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
         * Sets the start field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setStart(final DateTime value) {
            _start = value;
            return this;
        }

        /**
         * Sets the end field.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setEnd(final DateTime value) {
            _end = value;
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
        private DateTime _start;
        @NotNull
        private DateTime _end;
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

        public Unit getUnit2g() {
            return _unit;
        }

        public double getValue() {
            return _value;
        }

        private Sample(final Builder builder) {
            _unit = builder._unit;
            _value = builder._value;
        }

        private final Unit _unit;
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
            public Builder setUnit(final Unit value) {
                _unit = value;
                return this;
            }

            @NotNull
            private Double _value;
            private Unit _unit;
        }

        /**
         * 2G format Unit class.
         */
        public static final class Unit {

            public List<CompositeUnit> getNumerators() {
                return _numerators;
            }

            public List<CompositeUnit> getDenominators() {
                return _denominators;
            }

            private Unit(final Builder builder) {
                _numerators = builder._numerators == null
                        ? ImmutableList.of()
                        : ImmutableList.copyOf(builder._numerators);
                _denominators = builder._denominators == null
                        ? ImmutableList.of()
                        : ImmutableList.copyOf(builder._denominators);
            }

            private final List<CompositeUnit> _numerators;
            private final List<CompositeUnit> _denominators;

            /**
             * Builder for the 2G Unit class.
             */
            public static class Builder extends OvalBuilder<Unit> {
                /**
                 * Public constructor.
                 */
                public Builder() {
                    super(Unit::new);
                }
                /**
                 * Sets the unit numerators field.
                 *
                 * @param value Value
                 * @return This builder
                 */
                public Builder setNumerators(final List<CompositeUnit> value) {
                    _numerators = value;
                    return this;
                }

                /**
                 * Sets the unit denominators field.
                 *
                 * @param value Value
                 * @return This builder
                 */
                public Builder setDenominators(final List<CompositeUnit> value) {
                    _denominators = value;
                    return this;
                }

                private List<CompositeUnit> _numerators;
                private List<CompositeUnit> _denominators;
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

    /**
     * Composite Unit used in 2g file format.
     *
     * @author Ryan Ascheman (rascheman at groupon dot com)
     */
    public static class CompositeUnit {
        /**
         * Default constructor for Composite Unit.
         */
        public CompositeUnit() {}

        /**
         * Constructor for Composite Unit.
         *
         * @param scale Scale of the unit
         * @param type Type fo the unit
         */
        public CompositeUnit(final Scale scale, final Type type) {
            _type = type;
            _scale = scale;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj != null && obj instanceof CompositeUnit) {
                final CompositeUnit compositeUnit = (CompositeUnit) obj;
                return _type == compositeUnit._type && _scale == compositeUnit._scale;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return _type.hashCode() ^ (_scale != null ? _scale.hashCode() : 1);
        }

        public void setType(final Type value) {
            _type = value;
        }

        public void setScale(final Scale value) {
            _scale = value;
        }

        /**
         * Composite unit data type.
         */
        public enum Type {
            /*****************************************************************
             * Time
             */
            /**
             * Second.
             */
            SECOND(Category.TIME, 1),
            /**
             * Minute, 60 seconds.
             */
            MINUTE(Category.TIME, 60),
            /**
             * Hour, 60 minutes.
             */
            HOUR(Category.TIME, 60 * 60),
            /**
             * Day, 24 hours.
             */
            DAY(Category.TIME, 60 * 60 * 24),
            /**
             * Week, 7 days.
             */
            WEEK(Category.TIME, 60 * 60 * 24 * 7),
            /*****************************************************************
             * Data Size
             */
            /**
             * Bit.
             */
            BIT(Category.DATA_SIZE, 1),
            /**
             * Byte, 8 bits.
             */
            BYTE(Category.DATA_SIZE, 8),
            /*****************************************************************
             * Rotation
             */
            /**
             * Rotation.
             */
            ROTATION(Category.ROTATION, 1),
            /**
             * Degree, 360 = 1 Rotation.
             */
            DEGREE(Category.ROTATION, 360),
            /**
             * Radian, 2PI = 1 Rotation.
             */
            RADIAN(Category.ROTATION, 2 * Math.PI),
            /*****************************************************************
             * Temperature
             */
            /**
             * Celsius.
             */
            CELSIUS(Category.TEMPERATURE, 1),
            /**
             * Fahrenheit.
             */
            FAHRENHEIT(Category.TEMPERATURE, 1),
            /**
             * Kelvin.
             */
            KELVIN(Category.TEMPERATURE, 1);

            Type(final Category category, final double scaler) {
                _category = category;
                _scaler = scaler;
            }

            private Category _category;
            private double _scaler;
        }

        /**
         * Scalar values.
         */
        public enum Scale {
            /**
             * 10^-24.
             */
            YOCTO,
            /**
             * 10^-21.
             */
            ZEPTO,
            /**
             * 10^-18.
             */
            ATTO,
            /**
             * 10^-15.
             */
            FEMTO,
            /**
             * 10^-12.
             */
            PICO,
            /**
             * 10^-9.
             */
            NANO,
            /**
             * 10^-6.
             */
            MICRO,
            /**
             * 10^-3.
             */
            MILLI,
            /**
             * 10^-2.
             */
            CENTI,
            /**
             * 10^-1.
             */
            DECI,
            /**
             * 1.
             */
            ONE,
            /**
             * 10.
             */
            DECA,
            /**
             * 10^2.
             */
            HECTO,
            /**
             * 10^3.
             */
            KILO,
            /**
             * 10^6.
             */
            MEGA,
            /**
             * 10^9.
             */
            GIGA,
            /**
             * 10^12.
             */
            TERA,
            /**
             * 10^15.
             */
            PETA,
            /**
             * 10^18.
             */
            EXA,
            /**
             * 10^21.
             */
            ZETTA,
            /**
             * 10^24.
             */
            YOTTA,
            /**
             * 2^10.
             */
            KIBI,
            /**
             * 2^20.
             */
            MEBI,
            /**
             * 2^30.
             */
            GIBI,
            /**
             * 2^40.
             */
            TEBI,
            /**
             * 2^50.
             */
            PEBI,
            /**
             * 2^60.
             */
            EXBI,
            /**
             * 2^70.
             */
            ZEBI,
            /**
             * 2^80.
             */
            YOBI;
        }

        enum Category {
            TIME,
            DATA_SIZE,
            TEMPERATURE,
            ROTATION
        }

        @NotNull
        private Type _type;
        private Scale _scale;

        /**
         * Get the existing <code>Unit</code> that corresponds to the compound unit, or null if the compound unit has no
         * <code>Unit</code> analogue.
         *
         * @param compositeUnit The <code>CompoundUnit</code> for which to find the existing analogue.
         * @return The existing <code>Unit</code> to which the <code>CompoundUnit</code> maps.
         */
        @Nullable
        public static Unit getLegacyUnit(final CompositeUnit compositeUnit) {
            return LEGACY_UNIT_MAP.getOrDefault(compositeUnit, null);
        }

        private static final ImmutableMap<CompositeUnit, Unit> LEGACY_UNIT_MAP = new ImmutableMap.Builder<CompositeUnit, Unit>()
                .put(new CompositeUnit(CompositeUnit.Scale.NANO, CompositeUnit.Type.SECOND), Unit.NANOSECOND)
                .put(new CompositeUnit(CompositeUnit.Scale.MICRO, CompositeUnit.Type.SECOND), Unit.MICROSECOND)
                .put(new CompositeUnit(CompositeUnit.Scale.MILLI, CompositeUnit.Type.SECOND), Unit.MILLISECOND)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.SECOND), Unit.SECOND)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.MINUTE), Unit.MINUTE)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.HOUR), Unit.HOUR)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.DAY), Unit.DAY)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.WEEK), Unit.WEEK)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.BIT), Unit.BIT)
                .put(new CompositeUnit(CompositeUnit.Scale.KILO, CompositeUnit.Type.BIT), Unit.KILOBIT)
                .put(new CompositeUnit(CompositeUnit.Scale.MEGA, CompositeUnit.Type.BIT), Unit.MEGABIT)
                .put(new CompositeUnit(CompositeUnit.Scale.GIGA, CompositeUnit.Type.BIT), Unit.GIGABIT)
                .put(new CompositeUnit(CompositeUnit.Scale.TERA, CompositeUnit.Type.BIT), Unit.TERABIT)
                .put(new CompositeUnit(CompositeUnit.Scale.PETA, CompositeUnit.Type.BIT), Unit.PETABIT)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.BYTE), Unit.BYTE)
                .put(new CompositeUnit(CompositeUnit.Scale.KILO, CompositeUnit.Type.BYTE), Unit.KILOBYTE)
                .put(new CompositeUnit(CompositeUnit.Scale.MEGA, CompositeUnit.Type.BYTE), Unit.MEGABYTE)
                .put(new CompositeUnit(CompositeUnit.Scale.GIGA, CompositeUnit.Type.BYTE), Unit.GIGABYTE)
                .put(new CompositeUnit(CompositeUnit.Scale.TERA, CompositeUnit.Type.BYTE), Unit.TERABYTE)
                .put(new CompositeUnit(CompositeUnit.Scale.PETA, CompositeUnit.Type.BYTE), Unit.PETABYTE)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.KELVIN), Unit.KELVIN)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.CELSIUS), Unit.CELCIUS)
                .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.FAHRENHEIT), Unit.FAHRENHEIT)

                .put(new CompositeUnit(null, CompositeUnit.Type.SECOND), Unit.SECOND)
                .put(new CompositeUnit(null, CompositeUnit.Type.MINUTE), Unit.MINUTE)
                .put(new CompositeUnit(null, CompositeUnit.Type.HOUR), Unit.HOUR)
                .put(new CompositeUnit(null, CompositeUnit.Type.DAY), Unit.DAY)
                .put(new CompositeUnit(null, CompositeUnit.Type.WEEK), Unit.WEEK)
                .put(new CompositeUnit(null, CompositeUnit.Type.BIT), Unit.BIT)
                .put(new CompositeUnit(null, CompositeUnit.Type.BYTE), Unit.BYTE)
                .put(new CompositeUnit(null, CompositeUnit.Type.KELVIN), Unit.KELVIN)
                .put(new CompositeUnit(null, CompositeUnit.Type.CELSIUS), Unit.CELCIUS)
                .put(new CompositeUnit(null, CompositeUnit.Type.FAHRENHEIT), Unit.FAHRENHEIT)
                .build();
    }
}
