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
package com.arpnetworking.metrics.mad.model;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotNull;

import java.util.Objects;
import java.util.Optional;

/**
 * Default sample implementation.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@Loggable
public final class DefaultQuantity implements Quantity {

    @Override
    public double getValue() {
        return _value;
    }

    @Override
    public Optional<Unit> getUnit() {
        return _unit;
    }

    @Override
    public Quantity add(final Quantity otherQuantity) {
        if (_unit.isPresent() != otherQuantity.getUnit().isPresent()) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; thisQuantity=%s otherQuantity=%s",
                    this,
                    otherQuantity));
        }
        if (Objects.equals(_unit, otherQuantity.getUnit())) {
            return new DefaultQuantity(_value + otherQuantity.getValue(), _unit);
        }
        final Unit smallerUnit = _unit.get().getSmallerUnit(otherQuantity.getUnit().get());
        return new DefaultQuantity(
                smallerUnit.convert(_value, _unit.get())
                        + smallerUnit.convert(otherQuantity.getValue(), otherQuantity.getUnit().get()),
                Optional.of(smallerUnit));
    }

    @Override
    public Quantity subtract(final Quantity otherQuantity) {
        if (_unit.isPresent() != otherQuantity.getUnit().isPresent()) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; thisQuantity=%s otherQuantity=%s",
                    this,
                    otherQuantity));
        }
        if (Objects.equals(_unit, otherQuantity.getUnit())) {
            return new DefaultQuantity(_value - otherQuantity.getValue(), _unit);
        }
        final Unit smallerUnit = _unit.get().getSmallerUnit(otherQuantity.getUnit().get());
        return new DefaultQuantity(
                smallerUnit.convert(_value, _unit.get())
                        - smallerUnit.convert(otherQuantity.getValue(), otherQuantity.getUnit().get()),
                Optional.of(smallerUnit));
    }

    @Override
    public Quantity multiply(final Quantity otherQuantity) {
        if (_unit.isPresent() && otherQuantity.getUnit().isPresent()) {
            throw new UnsupportedOperationException("Compound units not supported yet");
        }
        return new DefaultQuantity(
                _value * otherQuantity.getValue(),
                Optional.ofNullable(_unit.orElse(otherQuantity.getUnit().orElse(null))));
    }

    @Override
    public Quantity divide(final Quantity otherQuantity) {
        // TODO(vkoskela): Support division by quantity with unit [2F].
        if (otherQuantity.getUnit().isPresent()) {
            throw new UnsupportedOperationException("Compound units not supported yet");
        }
        if (Objects.equals(_unit, otherQuantity.getUnit())) {
            return new DefaultQuantity(_value / otherQuantity.getValue(), Optional.empty());
        }
        return new DefaultQuantity(
                _value / otherQuantity.getValue(),
                _unit);
    }

    @Override
    public int compareTo(final Quantity other) {
        if (other.getUnit().equals(_unit)) {
            return Double.compare(_value, other.getValue());
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
        if (!(o instanceof DefaultQuantity)) {
            return false;
        }

        final DefaultQuantity sample = (DefaultQuantity) o;

        return Double.compare(sample.getValue(), _value) == 0
                && Objects.equals(_unit, sample.getUnit());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Unit", _unit)
                .add("Value", _value)
                .toString();
    }

    private DefaultQuantity(final Builder builder) {
        this(builder._value, Optional.ofNullable(builder._unit));
    }

    private DefaultQuantity(final double value, final Optional<Unit> unit) {
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
    public static final class Builder extends ThreadLocalBuilder<DefaultQuantity> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder, DefaultQuantity>) DefaultQuantity::new);
        }

        /**
         * Public constructor.
         *
         * @param quantity the <code>Quantity</code> to initialize from
         */
        public Builder(final DefaultQuantity quantity) {
            super((java.util.function.Function<Builder, DefaultQuantity>) DefaultQuantity::new);
            _value = quantity.getValue();
            _unit = quantity.getUnit().orElse(null);
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
        public DefaultQuantity build() {
            normalize();
            return new DefaultQuantity(this);
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
