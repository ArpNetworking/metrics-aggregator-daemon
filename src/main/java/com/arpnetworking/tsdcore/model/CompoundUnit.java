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
package com.arpnetworking.tsdcore.model;

import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotNull;


/**
 * Compound Unit used in 2g file format.
 *
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public class CompoundUnit {
    /**
     * Default constructor for Compound Unit.
     */
    public CompoundUnit() {}

    /**
     * Constructor for Compound Unit.
     *
     * @param scale Scale of the unit
     * @param type Type fo the unit
     */
    public CompoundUnit(final Scale scale, final Type type) {
        _type = type;
        _scale = scale;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj != null && obj instanceof CompoundUnit) {
            final CompoundUnit compoundUnit = (CompoundUnit) obj;
            return _type == compoundUnit._type && _scale == compoundUnit._scale;
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
     * Compound unit data type.
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
        YOCTO(10 ^ -24),
        /**
         * 10^-21.
         */
        ZEPTO(10 ^ -21),
        /**
         * 10^-18.
         */
        ATTO(10 ^ -18),
        /**
         * 10^-15.
         */
        FEMTO(10 ^ -15),
        /**
         * 10^-12.
         */
        PICO(10 ^ -12),
        /**
         * 10^-9.
         */
        NANO(10 ^ -9),
        /**
         * 10^-6.
         */
        MICRO(10 ^ -6),
        /**
         * 10^-3.
         */
        MILLI(10 ^ -3),
        /**
         * 10^-2.
         */
        CENTI(10 ^ -2),
        /**
         * 10^-1.
         */
        DECI(10 ^ -1),
        /**
         * 1.
         */
        ONE(1),
        /**
         * 10.
         */
        DECA(10),
        /**
         * 10^2.
         */
        HECTO(10 ^ 2),
        /**
         * 10^3.
         */
        KILO(10 ^ 3),
        /**
         * 10^6.
         */
        MEGA(10 ^ 6),
        /**
         * 10^9.
         */
        GIGA(10 ^ 9),
        /**
         * 10^12.
         */
        TERA(10 ^ 12),
        /**
         * 10^15.
         */
        PETA(10 ^ 15),
        /**
         * 10^18.
         */
        EXA(10 ^ 18),
        /**
         * 10^21.
         */
        ZETTA(10 ^ 21),
        /**
         * 10^24.
         */
        YOTTA(10 ^ 24),
        /**
         * 10^27.
         */
        KIBI(10 ^ 27),
        /**
         * 10^30.
         */
        MEBI(10 ^ 30),
        /**
         * 10^33.
         */
        GIBI(10 ^ 33),
        /**
         * 10^36.
         */
        TEBI(10 ^ 36),
        /**
         * 10^39.
         */
        PEBI(10 ^ 39),
        /**
         * 10^42.
         */
        EXBI(10 ^ 42),
        /**
         * 10^45.
         */
        ZEBI(10 ^ 45),
        /**
         * 10^48.
         */
        YOBI(10 ^ 48);

        /**
         * Constructor for Scale class.
         *
         * @param value numerical value of Scale for multiplication
         */
        Scale(final long value) {
            _value = value;
        }

        private long _value;
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

    public Unit getExistingUnit() {
        return gExistingUnitMap.getOrDefault(this, null);
    }

    /**
     * Get the existing unit that maps to the compound unit.
     *
     * @param compoundUnit newer unit format
     * @return existing unit which maps to the newer format
     */
    public static Unit getExistingUnit(final CompoundUnit compoundUnit) {
        if (null == compoundUnit) {
            return null;
        }

        return compoundUnit.getExistingUnit();
    }

    private static ImmutableMap<CompoundUnit, Unit> gExistingUnitMap = new ImmutableMap.Builder<CompoundUnit, Unit>()
            .put(new CompoundUnit(Scale.NANO, Type.SECOND), Unit.NANOSECOND)
            .put(new CompoundUnit(Scale.MICRO, Type.SECOND), Unit.MICROSECOND)
            .put(new CompoundUnit(Scale.MILLI, Type.SECOND), Unit.MILLISECOND)
            .put(new CompoundUnit(Scale.ONE, Type.SECOND), Unit.SECOND)
            .put(new CompoundUnit(Scale.ONE, Type.MINUTE), Unit.MINUTE)
            .put(new CompoundUnit(Scale.ONE, Type.HOUR), Unit.HOUR)
            .put(new CompoundUnit(Scale.ONE, Type.DAY), Unit.DAY)
            .put(new CompoundUnit(Scale.ONE, Type.WEEK), Unit.WEEK)
            .put(new CompoundUnit(Scale.ONE, Type.BIT), Unit.BIT)
            .put(new CompoundUnit(Scale.KILO, Type.BIT), Unit.KILOBIT)
            .put(new CompoundUnit(Scale.MEGA, Type.BIT), Unit.MEGABIT)
            .put(new CompoundUnit(Scale.GIGA, Type.BIT), Unit.GIGABIT)
            .put(new CompoundUnit(Scale.TERA, Type.BIT), Unit.TERABIT)
            .put(new CompoundUnit(Scale.PETA, Type.BIT), Unit.PETABIT)
            .put(new CompoundUnit(Scale.ONE, Type.BYTE), Unit.BYTE)
            .put(new CompoundUnit(Scale.KILO, Type.BYTE), Unit.KILOBYTE)
            .put(new CompoundUnit(Scale.MEGA, Type.BYTE), Unit.MEGABYTE)
            .put(new CompoundUnit(Scale.GIGA, Type.BYTE), Unit.GIGABYTE)
            .put(new CompoundUnit(Scale.TERA, Type.BYTE), Unit.TERABYTE)
            .put(new CompoundUnit(Scale.PETA, Type.BYTE), Unit.PETABYTE)
            .put(new CompoundUnit(Scale.ONE, Type.KELVIN), Unit.KELVIN)
            .put(new CompoundUnit(Scale.ONE, Type.CELSIUS), Unit.CELCIUS)
            .put(new CompoundUnit(Scale.ONE, Type.FAHRENHEIT), Unit.FAHRENHEIT)

            .put(new CompoundUnit(null, Type.SECOND), Unit.SECOND)
            .put(new CompoundUnit(null, Type.MINUTE), Unit.MINUTE)
            .put(new CompoundUnit(null, Type.HOUR), Unit.HOUR)
            .put(new CompoundUnit(null, Type.DAY), Unit.DAY)
            .put(new CompoundUnit(null, Type.WEEK), Unit.WEEK)
            .put(new CompoundUnit(null, Type.BIT), Unit.BIT)
            .put(new CompoundUnit(null, Type.BYTE), Unit.BYTE)
            .put(new CompoundUnit(null, Type.KELVIN), Unit.KELVIN)
            .put(new CompoundUnit(null, Type.CELSIUS), Unit.CELCIUS)
            .put(new CompoundUnit(null, Type.FAHRENHEIT), Unit.FAHRENHEIT)
            .build();
}
