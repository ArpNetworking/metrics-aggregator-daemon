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
package com.arpnetworking.configuration;

/**
 * Interface for classes which evaluate when to reload configuration.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public interface Trigger {

    /**
     * Evaluate the trigger. True indicates the trigger has been tripped.
     * Evaluating the trigger will reset its state and the trigger will not
     * return true again until it is tripped again.
     *
     * @return True if and only if the trigger has been tripped.
     */
    boolean evaluateAndReset();
}
