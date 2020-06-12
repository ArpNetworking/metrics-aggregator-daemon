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
package com.arpnetworking.metrics.mad.model.statistics;

import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.tsdcore.model.CalculatedValue;

/**
 * Specialization of {@link Calculator} directly supporting streaming
 * calculations over {@link Quantity} and {@link CalculatedValue}
 * streams.
 *
 * @param <T> The type of supporting data.
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public interface Accumulator<T> extends Calculator<T> {

    /**
     * Add the specified {@link Quantity} to the accumulated value. It is
     * permissible to mix calls to accumulate with {@link Quantity} and
     * {@link CalculatedValue}.
     *
     * @param quantity The {@link Quantity} to include in the accumulated value.
     * @return This {@link Accumulator}.
     */
    Accumulator<T> accumulate(Quantity quantity);

    /**
     * Add the specified {@link CalculatedValue} to the accumulated value. The
     * {@link CalculatedValue} was produced by this {@link Accumulator} in
     * a different context. For example, for a different time period or a different
     * host. It is permissible to mix calls to accumulate with {@link Quantity}
     * and {@link CalculatedValue}.
     *
     * @param calculatedValue The {@link CalculatedValue} to include in the accumulated value.
     * @return This {@link Accumulator}.
     */
    Accumulator<T> accumulate(CalculatedValue<T> calculatedValue);

    /**
     * Add the specified {@link CalculatedValue} to the accumulated value. The
     * {@link CalculatedValue} was produced by this {@link Accumulator} in
     * a different context. For example, for a different time period or a different
     * host. It is permissible to mix calls to accumulate with {@link Quantity}
     * and {@link CalculatedValue}.
     *
     * If the {@link CalculatedValue}'s supporting data is of an unsupported
     * type then an {@link IllegalArgumentException} will be thrown.
     *
     * @param calculatedValue The {@link CalculatedValue} to include in the accumulated value.
     * @return This {@link Accumulator}.
     */
    Accumulator<T> accumulateAny(CalculatedValue<?> calculatedValue);
}
