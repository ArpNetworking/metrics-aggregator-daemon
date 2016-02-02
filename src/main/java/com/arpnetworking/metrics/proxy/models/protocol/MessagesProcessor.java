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

import com.arpnetworking.metrics.Metrics;

/**
 * Defines simple interface for a message processor.
 *
 * @author Brandon Arp (barp at groupon dot com)
 */
public interface MessagesProcessor {
    /**
     * Process message.
     *
     * @param message message to be processed.
     * @return true if the message was processed, otherwise false
     */
    boolean handleMessage(final Object message);

    /**
     * Initializes Processor-owned periodic metrics counters to zero.
     *
     * @param metrics Metrics to reset counts in.
     */
    void initializeMetrics(final Metrics metrics);
}
