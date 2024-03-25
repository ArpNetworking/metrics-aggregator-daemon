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

import java.util.Optional;

/**
 * Represents a sample.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public interface Quantity extends Comparable<Quantity> {

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
     * Add this {@link Quantity} to the specified one returning the
     * result. Both {@link Quantity} instances must either not have a
     * {@link Unit} or the {@link Unit} must be of the same type.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting sum {@link Quantity}.
     */
    Quantity add(Quantity otherQuantity);

    /**
     * Subtract the specified {@link Quantity} from this one returning
     * the result. Both {@link Quantity} instances must either not have
     * a {@link Unit} or the {@link Unit} must be of the same type.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting difference {@link Quantity}.
     */
    Quantity subtract(Quantity otherQuantity);

    /**
     * Multiply this {@link Quantity} with the specified one returning
     * the result.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting product {@link Quantity}.
     */
    Quantity multiply(Quantity otherQuantity);

    /**
     * Divide this {@link Quantity} by the specified one returning
     * the result.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting quotient {@link Quantity}.
     */
    Quantity divide(Quantity otherQuantity);
}
