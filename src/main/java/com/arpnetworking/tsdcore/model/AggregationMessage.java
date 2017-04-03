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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessage;
import org.vertx.java.core.buffer.Buffer;

/**
 * Class for building messages from the raw, on-the-wire bytes in the TCP stream.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class AggregationMessage {

    /**
     * Static factory.
     *
     * @param message The message.
     * @return New <code>AggregationMessage</code> instance.
     */
    public static AggregationMessage create(final GeneratedMessage message) {
        return new AggregationMessage(message);
    }

    /**
     * Serialize the message into a <code>Buffer</code>.
     *
     * @return <code>Buffer</code> containing serialized message.
     */
    public Buffer serialize() {
        final Buffer b = new Buffer();
        b.appendInt(0);
        if (_message instanceof Messages.HostIdentification) {
            b.appendByte((byte) 0x01);
        } else if (_message instanceof Messages.HeartbeatRecord) {
            b.appendByte((byte) 0x03);
        } else if (_message instanceof Messages.StatisticSetRecord) {
            b.appendByte((byte) 0x04);
        } else if (_message instanceof Messages.SamplesSupportingData) {
            b.appendByte((byte) 0x05);
            b.appendByte((byte) 0x01);
        } else if (_message instanceof Messages.SparseHistogramSupportingData) {
            b.appendByte((byte) 0x05);
            b.appendByte((byte) 0x02);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported message; message=%s", _message));
        }
        b.appendBytes(_message.toByteArray());
        b.setInt(0, b.length());
        return b;
    }

    public GeneratedMessage getMessage() {
        return _message;
    }

    public int getLength() {
        return _message.getSerializedSize() + HEADER_SIZE_IN_BYTES;
    }

    private AggregationMessage(final GeneratedMessage message) {
        _message = message;
    }

    private final GeneratedMessage _message;

    private static final int INTEGER_SIZE_IN_BYTES = Integer.SIZE / 8;
    private static final int HEADER_SIZE_IN_BYTES = INTEGER_SIZE_IN_BYTES + 1;
}
