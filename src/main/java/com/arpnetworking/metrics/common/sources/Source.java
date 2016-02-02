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
package com.arpnetworking.metrics.common.sources;

import com.arpnetworking.commons.observer.Observable;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Interface for sources of <code>Record</code> data entries. All
 * implementations must be thread safe.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
public interface Source extends Observable {

    /**
     * Called to allow the source to start producing records.
     */
    void start();

    /**
     * Called to allow the source to clean-up. No further records should be
     * produced.
     */
    void stop();
}
