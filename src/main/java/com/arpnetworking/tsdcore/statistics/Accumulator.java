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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;

/**
 * Specialization of <code>Calculator</code> directly supporting streaming
 * calculations over <code>Quantity</code> and <code>CalculatedValue</code>
 * streams.
 *
 * @param <T> The type of supporting data.
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface Accumulator<T> extends Calculator<T> {

    /**
     * Add the specified <code>Quantity</code> to the accumulated value. It is
     * permissible to mix calls to accumulate with <code>Quantity</code> and
     * <code>CalculatedValue</code>.
     *
     * @param quantity The <code>Quantity</code> to include in the accumulated value.
     * @return This <code>Accumulator</code>.
     */
    Accumulator<T> accumulate(Quantity quantity);

    /**
     * Add the specified <code>CalculatedValue</code> to the accumulated value. The
     * <code>CalculatedValue</code> was produced by this <code>Accumulator</code> in
     * a different context. For example, for a different time period or a different
     * host. It is permissible to mix calls to accumulate with <code>Quantity</code>
     * and <code>CalculatedValue</code>.
     *
     * @param calculatedValue The <code>CalculatedValue</code> to include in the accumulated value.
     * @return This <code>Accumulator</code>.
     */
    Accumulator<T> accumulate(CalculatedValue<T> calculatedValue);
}
