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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import net.sf.oval.constraint.NotNull;

/**
 * Represents a value that is reaggregatable.
 *
 * @param <T> The type of supporting data.
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class CalculatedValue<T> {

    public Quantity getValue() {
        return _value;
    }

    public T getData() {
        return _data;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof CalculatedValue)) {
            return false;
        }

        final CalculatedValue<?> otherCalculatedValue = (CalculatedValue<?>) other;
        return Objects.equal(getValue(), otherCalculatedValue.getValue())
                && Objects.equal(getData(), otherCalculatedValue.getData());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getValue(), getData());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Value", _value)
                .add("Data", _data)
                .toString();
    }

    private CalculatedValue(final Builder<T> builder) {
        _value = builder._value;
        _data = builder._data;
    }

    private final Quantity _value;
    private final T _data;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link CalculatedValue}.
     *
     * @param <T> type of the object to be built
     */
    public static final class Builder<T> extends ThreadLocalBuilder<CalculatedValue<T>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder<T>, CalculatedValue<T>>) CalculatedValue::new);
        }

        /**
         * Set the value. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder<T> setValue(final Quantity value) {
            _value = value;
            return this;
        }

        /**
         * Set the data. Optional. Defaults to null.
         *
         * @param data The data.
         * @return This {@link Builder} instance.
         */
        public Builder<T> setData(final T data) {
            _data = data;
            return this;
        }

        @Override
        protected void reset() {
            _value = null;
            _data = null;
        }

        @NotNull
        private Quantity _value;
        private T _data;
    }
}
