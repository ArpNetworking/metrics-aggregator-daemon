/*
 * Copyright 2017 Inscope Metrics Inc.
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

/**
 * The unit type classification.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public enum UnitType {
    /**
     * Used in describing time durations.
     */
    TIME {
        @Override
        public Unit getDefaultUnit() {
            return Unit.SECOND;
        }
    },
    /**
     * Used in describing data sizes.
     */
    DATA_SIZE {
        @Override
        public Unit getDefaultUnit() {
            return Unit.BYTE;
        }
    },
    /**
     * Used in describing measurements of temperature.
     */
    TEMPERATURE {
        @Override
        public Unit getDefaultUnit() {
            return Unit.KELVIN;
        }
    };

    /**
     * Get the default unit for this unit type. Used in normalizing values.
     *
     * @return The default unit for a given domain.
     */
    public abstract Unit getDefaultUnit();
}
