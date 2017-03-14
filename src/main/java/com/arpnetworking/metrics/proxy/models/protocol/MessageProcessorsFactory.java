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

package com.arpnetworking.metrics.proxy.models.protocol;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.proxy.actors.Connection;

import java.util.List;

/**
 * A factory that creates a <code>List</code> of <code>MessagesProcessor</code> that
 * implement a protocol version.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public interface MessageProcessorsFactory {
    /**
     * Create a list of <code>MessagesProcessor</code> that define a protocol.
     *
     * @param connection the connection context to use for message processing
     * @param metrics {@link PeriodicMetrics} instance to record metrics to
     * @return a list of <code>MessagesProcessor</code>
     */
    List<MessagesProcessor> create(final Connection connection, final PeriodicMetrics metrics);
}
