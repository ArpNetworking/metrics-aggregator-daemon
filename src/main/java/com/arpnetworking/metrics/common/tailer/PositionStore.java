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
package com.arpnetworking.metrics.common.tailer;

import java.io.Closeable;
import java.util.Optional;

/**
 * Interface to storage for tracking the read position in a file.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public interface PositionStore extends Closeable {

    /**
     * Get the read location in the file identified by a hash.
     *
     * @param identifier unique identifier for the file.
     * @return Optional.absent if the file was not found, otherwise a byte offset
     */
    Optional<Long> getPosition(String identifier);

    /**
     * Update the read offset from the beginning of the file for the specified
     * file identifier.
     *
     * @param identifier unique identifier for the file.
     * @param position the new read offset from the beginning of the file in bytes.
     */
    void setPosition(String identifier, long position);
}
