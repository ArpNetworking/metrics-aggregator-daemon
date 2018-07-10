/*
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
package com.inscopemetrics.mad.model;

import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessageV3;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Class for building on-the-wire bytes for messages.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class AggregationMessage {

    /**
     * Static factory.
     *
     * @param message The message.
     * @return New ${link AggregationMessage} instance.
     */
    public static AggregationMessage create(final GeneratedMessageV3 message) {
        return new AggregationMessage(message);
    }

    /**
     * Serialize the message into a ${code ByteString}.
     *
     * @return ${code ByteString} containing serialized message.
     */
    public ByteString serializeToByteString() {
        final ByteStringBuilder b = ByteString.createBuilder();
        if (_message instanceof Messages.HostIdentification) {
            b.putByte((byte) 0x01);
        } else if (_message instanceof Messages.HeartbeatRecord) {
            b.putByte((byte) 0x03);
        } else if (_message instanceof Messages.StatisticSetRecord) {
            b.putByte((byte) 0x04);
        } else if (_message instanceof Messages.SamplesSupportingData) {
            b.putByte((byte) 0x05);
            b.putByte((byte) 0x01);
        } else if (_message instanceof Messages.SparseHistogramSupportingData) {
            b.putByte((byte) 0x05);
            b.putByte((byte) 0x02);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unsupported message; messageClass=%s", _message.getClass()));
        }
        try {
            _message.writeTo(b.asOutputStream());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final ByteString bs = b.result();
        final ByteStringBuilder sizePrefix = ByteString.createBuilder();
        sizePrefix.putInt(bs.size() + INTEGER_SIZE_IN_BYTES, ByteOrder.BIG_ENDIAN);
        return sizePrefix.result().concat(bs);
    }

    public GeneratedMessageV3 getMessage() {
        return _message;
    }

    public int getLength() {
        return _message.getSerializedSize() + HEADER_SIZE_IN_BYTES;
    }

    private AggregationMessage(final GeneratedMessageV3 message) {
        _message = message;
    }

    private final GeneratedMessageV3 _message;

    private static final int INTEGER_SIZE_IN_BYTES = Integer.SIZE / 8;
    private static final int HEADER_SIZE_IN_BYTES = INTEGER_SIZE_IN_BYTES + 1;
}
