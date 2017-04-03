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

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.proxy.actors.Connection;
import com.arpnetworking.metrics.proxy.models.protocol.MessageProcessorsFactory;
import com.arpnetworking.metrics.proxy.models.protocol.MessagesProcessor;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Factory for creating all the processors needed to process a v1 message.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ProcessorsV1Factory implements MessageProcessorsFactory {
    @Override
    public List<MessagesProcessor> create(final Connection connection, final PeriodicMetrics metrics) {
        return Lists.newArrayList(
                new HeartbeatMessagesProcessor(connection, metrics),
                new LogMessagesDroppingProcessor(),
                new MetricMessagesProcessor(connection, metrics));
    }
}
