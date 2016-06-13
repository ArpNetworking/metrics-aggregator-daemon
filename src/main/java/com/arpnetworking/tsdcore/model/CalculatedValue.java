/**
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

import com.arpnetworking.commons.builder.OvalBuilder;
import net.sf.oval.constraint.NotNull;

/**
 * Represents a value that is reaggregatable.
 *
 * @param <T> The type of supporting data.
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class CalculatedValue<T> {

    public Quantity getValue() {
        return _value;
    }

    public T getData() {
        return _data;
    }

    private CalculatedValue(final Builder<T> builder) {
        _value = builder._value;
        _data = builder._data;
    }

    private final Quantity _value;
    private final T _data;

    /**
     * <code>Builder</code> implementation for <code>CalculatedValue</code>.
     *
     * @param <T> type of the object to be built
     */
    public static final class Builder<T> extends OvalBuilder<CalculatedValue<T>> {

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
         * @return This <code>Builder</code> instance.
         */
        public Builder<T> setValue(final Quantity value) {
            _value = value;
            return this;
        }

        /**
         * Set the data. Optional. Cannot be null. Defaults to empty list.
         *
         * @param data The data.
         * @return This <code>Builder</code> instance.
         */
        public Builder<T> setData(final T data) {
            _data = data;
            return this;
        }

        @NotNull
        private Quantity _value;
        private T _data;
    }
}
