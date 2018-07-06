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
package com.inscopemetrics.tsdcore.model;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a sample.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class Quantity implements Comparable<Quantity>, Serializable {

    public double getValue() {
        return _value;
    }

    public Optional<Unit> getUnit() {
        return _unit;
    }

    /**
     * Add this <code>Quantity</code> to the specified one returning the
     * result. Both <code>Quantity</code> instances must either not have a
     * <code>Unit</code> or the <code>Unit</code> must be of the same type.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting sum <code>Quantity</code>.
     */
    public Quantity add(final Quantity otherQuantity) {
        if (_unit.isPresent() != otherQuantity._unit.isPresent()) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; thisQuantity=%s otherQuantity=%s",
                    this,
                    otherQuantity));
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value + otherQuantity._value, _unit);
        }
        final Unit smallerUnit = _unit.get().getSmallerUnit(otherQuantity.getUnit().get());
        return new Quantity(
                smallerUnit.convert(_value, _unit.get())
                        + smallerUnit.convert(otherQuantity._value, otherQuantity._unit.get()),
                Optional.of(smallerUnit));
    }

    /**
     * Subtract the specified <code>Quantity</code> from this one returning
     * the result. Both <code>Quantity</code> instances must either not have
     * a <code>Unit</code> or the <code>Unit</code> must be of the same type.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting difference <code>Quantity</code>.
     */
    public Quantity subtract(final Quantity otherQuantity) {
        if (_unit.isPresent() != otherQuantity._unit.isPresent()) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; thisQuantity=%s otherQuantity=%s",
                    this,
                    otherQuantity));
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value - otherQuantity._value, _unit);
        }
        final Unit smallerUnit = _unit.get().getSmallerUnit(otherQuantity.getUnit().get());
        return new Quantity(
                smallerUnit.convert(_value, _unit.get())
                        - smallerUnit.convert(otherQuantity._value, otherQuantity._unit.get()),
                Optional.of(smallerUnit));
    }

    /**
     * Multiply this <code>Quantity</code> with the specified one returning
     * the result.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting product <code>Quantity</code>.
     */
    public Quantity multiply(final Quantity otherQuantity) {
        if (_unit.isPresent() && otherQuantity._unit.isPresent()) {
            throw new UnsupportedOperationException("Compound units not supported yet");
        }
        return new Quantity(
                _value * otherQuantity._value,
                Optional.ofNullable(_unit.orElse(otherQuantity._unit.orElse(null))));
    }

    /**
     * Divide this <code>Quantity</code> by the specified one returning
     * the result.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting quotient <code>Quantity</code>.
     */
    public Quantity divide(final Quantity otherQuantity) {
        // TODO(vkoskela): Support division by quantity with unit [2F].
        if (otherQuantity._unit.isPresent()) {
            throw new UnsupportedOperationException("Compound units not supported yet");
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value / otherQuantity._value, Optional.empty());
        }
        return new Quantity(
                _value / otherQuantity._value,
                _unit);
    }

    @Override
    public int compareTo(final Quantity other) {
        if (other._unit.equals(_unit)) {
            return Double.compare(_value, other._value);
        }
        throw new IllegalArgumentException(String.format(
                "Cannot compare mismatched units; this=%s, other=%s",
                this,
                other));
    }

    @Override
    public int hashCode() {
        return Objects.hash(_value, _unit);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Quantity)) {
            return false;
        }

        final Quantity sample = (Quantity) o;

        return Double.compare(sample._value, _value) == 0
                && Objects.equals(_unit, sample._unit);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Unit", _unit)
                .add("Value", _value)
                .toString();
    }

    private Quantity(final Builder builder) {
        this(builder._value, Optional.ofNullable(builder._unit));
    }

    private Quantity(final double value, final Optional<Unit> unit) {
        _value = value;
        _unit = unit;
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Optional<Unit> _unit;
    private final double _value;

    private static final long serialVersionUID = -6339526234042605516L;

    /**
     * <code>Builder</code> implementation for <code>Quantity</code>.
     */
    public static final class Builder extends ThreadLocalBuilder<Quantity> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder, Quantity>) Quantity::new);
        }

        /**
         * Public constructor.
         *
         * @param quantity the <code>Quantity</code> to initialize from
         */
        public Builder(final Quantity quantity) {
            super((java.util.function.Function<Builder, Quantity>) Quantity::new);
            _value = quantity._value;
            _unit = quantity._unit.orElse(null);
        }

        /**
         * Set the value. Required. Cannot be null.
         *
         * @param value The value.
         * @return This <code>Builder</code> instance.
         */
        public Builder setValue(final Double value) {
            _value = value;
            return this;
        }

        /**
         * Set the unit. Optional. Default is no unit.
         *
         * @param value The unit.
         * @return This <code>Builder</code> instance.
         */
        public Builder setUnit(final Unit value) {
            _unit = value;
            return this;
        }

        @Override
        public Quantity build() {
            normalize();
            return new Quantity(this);
        }

        @Override
        protected void reset() {
            _value = null;
            _unit = null;
        }

        private Builder normalize() {
            if (_value != null && _unit != null) {
                final Unit defaultUnit = _unit.getType().getDefaultUnit();
                if (!Objects.equals(_unit, defaultUnit)) {
                    _value = defaultUnit.convert(_value, _unit);
                    _unit = defaultUnit;
                }
            }
            return this;
        }

        @NotNull
        private Double _value;
        private Unit _unit;
    }
}
