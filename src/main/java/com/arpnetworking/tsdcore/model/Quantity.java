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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a sample.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
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
        if (_unit.equals(otherQuantity._unit)) {
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
        if (_unit.equals(otherQuantity._unit)) {
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
        // TODO(vkoskela): Support division by quantity with unit [2F].
        if (otherQuantity._unit.isPresent()) {
            throw new UnsupportedOperationException("Compount units not supported yet");
        }
        if (_unit.equals(otherQuantity._unit)) {
            return new Quantity(_value * otherQuantity._value, Optional.absent());
        }
        return new Quantity(
                _value * otherQuantity._value,
                _unit);
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
            throw new UnsupportedOperationException("Compount units not supported yet");
        }
        if (_unit.equals(otherQuantity._unit)) {
            return new Quantity(_value / otherQuantity._value, Optional.absent());
        }
        return new Quantity(
                _value / otherQuantity._value,
                _unit);
    }

    /**
     * Convert this <code>Quantity</code> to one in the specified unit. This
     * <code>Quantity</code> must also have a <code>Unit</code> and it must
     * be in the same domain as the provided unit.
     *
     * @param unit <code>Unit</code> to convert to.
     * @return <code>Quantity</code> in specified unit.
     */
    public Quantity convertTo(final Unit unit) {
        if (!_unit.isPresent()) {
            throw new IllegalStateException(String.format(
                    "Cannot convert a quantity without a unit; this=%s",
                    this));
        }
        if (_unit.get().equals(unit)) {
            return this;
        }
        return new Quantity(
                unit.convert(_value, _unit.get()),
                Optional.of(unit));
    }

    /**
     * Convert this <code>Quantity</code> to one in the specified optional unit.
     * Either this <code>Quantity</code> also has a <code>Unit</code> in the
     * same domain as the provided unit or both units are absent.
     *
     * @param unit <code>Optional</code> <code>Unit</code> to convert to.
     * @return <code>Quantity</code> in specified unit.
     */
    public Quantity convertTo(final Optional<Unit> unit) {
        if (_unit.isPresent() != unit.isPresent()) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; quantity=%s unit=%s",
                    this,
                    unit));
        }
        if (_unit.equals(unit)) {
            return this;
        }
        return new Quantity(
                unit.get().convert(_value, _unit.get()),
                unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(final Quantity other) {
        if (other._unit.equals(_unit)) {
            return Double.compare(_value, other._value);
        } else if (other._unit.isPresent() && _unit.isPresent()) {
            final Unit smallerUnit = _unit.get().getSmallerUnit(other._unit.get());
            final double convertedValue = smallerUnit.convert(_value, _unit.get());
            final double otherConvertedValue = smallerUnit.convert(other._value, other._unit.get());
            return Double.compare(convertedValue, otherConvertedValue);
        }
        throw new IllegalArgumentException(String.format(
                "Cannot compare a quantity with a unit to a quantity without a unit; this=%s, other=%s",
                this,
                other));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(_value, _unit);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Unit", _unit)
                .add("Value", _value)
                .toString();
    }

    /**
     * Ensures all <code>Quantity</code> instances have the same (including no)
     * <code>Unit</code>.
     *
     * @param quantities <code>Quantity</code> instances to convert.
     * @return <code>List</code> of <code>Quantity</code> instances with the
     * same <code>Unit</code> (or no <code>Unit</code>).
     */
    public static List<Quantity> unify(final Collection<Quantity> quantities) {
        // This is a 2-pass operation:
        // First pass is to grab the smallest unit in the samples
        // Second pass is to convert everything to that unit
        Optional<Unit> smallestUnit = Optional.absent();
        for (final Quantity quantity : quantities) {
            if (!smallestUnit.isPresent()) {
                smallestUnit = quantity.getUnit();
            } else if (quantity.getUnit().isPresent()
                    && !smallestUnit.get().getSmallerUnit(quantity.getUnit().get()).equals(smallestUnit.get())) {
                smallestUnit = quantity.getUnit();
            }
        }

        final List<Quantity> convertedSamples = Lists.newArrayListWithExpectedSize(quantities.size());
        final Function<Quantity, Quantity> converter = SampleConverter.to(smallestUnit);
        for (final Quantity quantity : quantities) {
            convertedSamples.add(converter.apply(quantity));
        }

        return convertedSamples;
    }

    private Quantity(final Builder builder) {
        this(builder._value, Optional.fromNullable(builder._unit));
    }

    private Quantity(final double value, final Optional<Unit> unit) {
        _value = value;
        _unit = unit;
    }

    private final Optional<Unit> _unit;
    private final double _value;

    private static final long serialVersionUID = -6339526234042605516L;

    private static final class SampleConverter implements Function<Quantity, Quantity> {

        public static Function<Quantity, Quantity> to(final Optional<Unit> unit) {
            if (unit.isPresent()) {
                return new SampleConverter(unit.get());
            }
            return (q) -> q;
        }

        @Override
        @Nullable
        public Quantity apply(@Nullable final Quantity quantity) {
            if (quantity == null) {
                return null;
            }
            if (!quantity.getUnit().isPresent()) {
                throw new IllegalArgumentException(String.format("Cannot convert a quantity without unit; sample=%s", quantity));
            }
            return new Quantity.Builder()
                    .setValue(_unit.convert(quantity.getValue(), quantity.getUnit().get()))
                    .setUnit(_unit)
                    .build();
        }

        private SampleConverter(final Unit convertTo) {
            _unit = convertTo;
        }

        private final Unit _unit;
    }

    /**
     * <code>Builder</code> implementation for <code>Quantity</code>.
     */
    public static final class Builder extends OvalBuilder<Quantity> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder, Quantity>) Quantity::new);
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

        /**
         * {@inheritDoc}
         */
        @Override
        public Quantity build() {
            if (_value == null) {
                throw new IllegalStateException("value cannot be null");
            }
            return new Quantity(this);
        }

        @NotNull
        private Double _value;
        private Unit _unit;
    }
}
