/*
 * Copyright 2019 Dropbox
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

import java.io.Serializable;
import java.util.Optional;

/**
 * Represents a sample.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public interface Quantity extends Comparable<Quantity>, Serializable {

    /**
     * Access the {@link Quantity} value.
     *
     * @return the {@link Quantity} value
     */
    double getValue();

    /**
     * Access the {@link Quantity} {@link Unit}.
     *
     * @return the {@link Quantity} {@link Unit}
     */
    Optional<Unit> getUnit();

    /**
     * Add this <code>Quantity</code> to the specified one returning the
     * result. Both <code>Quantity</code> instances must either not have a
     * <code>Unit</code> or the <code>Unit</code> must be of the same type.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting sum <code>Quantity</code>.
     */
    Quantity add(Quantity otherQuantity);

    /**
     * Subtract the specified <code>Quantity</code> from this one returning
     * the result. Both <code>Quantity</code> instances must either not have
     * a <code>Unit</code> or the <code>Unit</code> must be of the same type.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting difference <code>Quantity</code>.
     */
    Quantity subtract(Quantity otherQuantity);

    /**
     * Multiply this <code>Quantity</code> with the specified one returning
     * the result.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting product <code>Quantity</code>.
     */
    Quantity multiply(Quantity otherQuantity);

    /**
     * Divide this <code>Quantity</code> by the specified one returning
     * the result.
     *
     * @param otherQuantity The other <code>Quantity</code>.
     * @return The resulting quotient <code>Quantity</code>.
     */
    Quantity divide(Quantity otherQuantity);
}
