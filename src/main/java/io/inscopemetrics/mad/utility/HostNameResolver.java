/*
 * Copyright 2018 Inscope Metrics Inc.
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

package io.inscopemetrics.mad.utility;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Interface for classes which resolve host names. This is commonly used
 * to inject a service discovery or load balancing strategy that is not
 * supported transparently through DNS lookup.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
public interface HostNameResolver {

    /**
     * Resolve a host name to another hostname or ip address.
     *
     * @param hostName the host name to resolve.
     * @return the resolved host name or ip address.
     */
    String resolve(String hostName);
}
