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
package com.arpnetworking.metrics.proxy.models.protocol.v1;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.proxy.models.messages.LogLine;
import com.arpnetworking.metrics.proxy.models.messages.LogsList;
import com.arpnetworking.metrics.proxy.models.messages.NewLog;
import com.arpnetworking.metrics.proxy.models.protocol.MessagesProcessor;

/**
 * Drops all log messages so they aren't "unhandled" and DOS us.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class LogMessagesDroppingProcessor implements MessagesProcessor {
    /**
     * Process message.
     *
     * @param message message to be processed.
     * @return true if the message was processed, otherwise false
     */
    @Override
    public boolean handleMessage(final Object message) {
        return message instanceof LogLine
                || message instanceof NewLog
                || message instanceof LogsList;
    }

    /**
     * Initializes Processor-owned periodic metrics counters to zero.
     *
     * @param metrics Metrics to reset counts in.
     */
    @Override
    public void initializeMetrics(final Metrics metrics) { }
}
