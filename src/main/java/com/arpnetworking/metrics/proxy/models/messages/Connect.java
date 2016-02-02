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

package com.arpnetworking.metrics.proxy.models.messages;

import akka.actor.ActorRef;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;

/**
 * Akka message to hold new connection data.
 *
 * @author Brandon Arp (barp at groupon dot com)
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@Loggable
public final class Connect {
    /**
     * Public constructor.
     *
     * @param telemetry Actor reference to the {@code Telemetry} actor.
     * @param connection Actor reference to the {@code Connection} actor.
     * @param channel Actor reference to the {@code Source<Message, ActorRef>} actor.
     */
    public Connect(final ActorRef telemetry, final ActorRef connection, final ActorRef channel) {
        _telemetry = telemetry;
        _connection = connection;
        _channel = channel;
    }

    public ActorRef getTelemetry() {
        return _telemetry;
    }

    public ActorRef getConnection() {
        return _connection;
    }

    public ActorRef getChannel() {
        return _channel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("Telemetry", _telemetry)
                .add("Connection", _connection)
                .add("Channel", _channel)
                .toString();
    }

    private final ActorRef _telemetry;
    private final ActorRef _connection;
    private final ActorRef _channel;
}
