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
 * Defines a factory for creating {@link com.arpnetworking.utility.Launchable} objects from a config object.
 *
 * @param <T> The type of the {@link com.arpnetworking.utility.Launchable}.
 * @param <S> The type of config.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public interface ConfiguredLaunchableFactory<T, S> {
    /**
     * Creates a {@link com.arpnetworking.utility.Launchable}.
     *
     * @param config configuration used to build the {@link com.arpnetworking.utility.Launchable}
     * @return a new {@link com.arpnetworking.utility.Launchable}
     */
    T create(S config);
}
