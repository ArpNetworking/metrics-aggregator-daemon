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
package com.arpnetworking.configuration.jackson;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

/**
 * Interface for sourcing <code>JsonNode</code> based configuration.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface JsonNodeSource {

    /**
     * Retrieve the <code>JsonNode</code> by looking up the sequence of keys or
     * return <code>Optional.absent()</code> if any key in the sequence does
     * not exist.
     *
     * @param keys The sequence of keys to look-up to find the value.
     * @return The <code>JsonNode</code> representing the value.
     */
    Optional<JsonNode> getValue(String... keys);
}
