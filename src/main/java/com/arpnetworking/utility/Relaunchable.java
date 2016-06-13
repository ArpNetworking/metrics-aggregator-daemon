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
package com.arpnetworking.utility;

/**
 * Interface for components which can be restarted.
 *
 * @param <T> The type representing the validated configuration.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface Relaunchable<T> {

    /**
     * Relaunch the component.
     *
     * @param configuration The configuration instance.
     */
    void relaunch(final T configuration);
}
