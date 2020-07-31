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
package com.arpnetworking.tsdcore.model;

import akka.util.ByteIterator;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Class for building on-the-wire bytes for messages.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
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
     * Serialize the message into a ${code Buffer}.
     *
     * @return ${code Buffer} containing serialized message.
     */
    public Buffer serializeToBuffer() {
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

    /**
     * Serialize the message into a ${code ByteString}.
     *
     * @return ${code Buffer} containing serialized message.
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


    /**
     * Deserialize message from {@link Buffer}.
     *
     * @param data The {@link Buffer} containing the serialized message.
     * @return The deserialized {@link AggregationMessage} or absent if
     * the {@link Buffer} could not be deserialized.
     */
    public static Optional<AggregationMessage> deserialize(final ByteString data) {
        int position = 0;
        // Make sure we have enough data to get the size
        if (data.size() < HEADER_SIZE_IN_BYTES) {
            return Optional.empty();
        }

        // Deserialize and validate buffer length
        final ByteIterator reader = data.iterator();
        final int length = reader.getInt(ByteOrder.BIG_ENDIAN);
        position += INTEGER_SIZE_IN_BYTES;
        if (data.size() < length) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("we only have %d of %d bytes.", data.size(), length));
            }
            return Optional.empty();
        }

        // Deserialize message type
        final byte type = reader.getByte();
        position += BYTE_SIZE_IN_BYTES;

        final byte subType;
        if (typeHasSubtype(type)) {
            subType = reader.getByte();
            position += BYTE_SIZE_IN_BYTES;
        } else {
            subType = 0x00;
        }

        // Obtain the serialized payload
        final byte[] payloadBytes = new byte [length - position];
        reader.getBytes(payloadBytes);

        // Deserialize the message based on the type
        try {
            switch (type) {
                case 0x01:
                    return Optional.of(new AggregationMessage(Messages.HostIdentification.parseFrom(payloadBytes)));
                case 0x03:
                    return Optional.of(new AggregationMessage(Messages.HeartbeatRecord.parseFrom(payloadBytes)));
                case 0x04:
                    return Optional.of(new AggregationMessage(Messages.StatisticSetRecord.parseFrom(payloadBytes)));
                case 0x05:
                    // 0x05 is the message type for all supporting data
                    switch (subType) {
                        case 0x01:
                            return Optional.of(new AggregationMessage(Messages.SamplesSupportingData.parseFrom(payloadBytes)));
                        case 0x02:
                            return Optional.of(new AggregationMessage(Messages.SparseHistogramSupportingData.parseFrom(payloadBytes)));
                        default:
                            LOGGER.warn(
                                    String.format("Invalid protocol buffer, unknown subtype; type=%s, subtype=%s, bytes=%s",
                                            type,
                                            subType,
                                            Hex.encodeHexString(payloadBytes)));
                            return Optional.empty();
                    }
                default:
                    LOGGER.warn(String.format("Unsupported message type; type=%s", type));
                    return Optional.empty();
            }
        } catch (final InvalidProtocolBufferException e) {
            LOGGER.warn(
                    String.format("Invalid protocol buffer; type=%s bytes=%s", type, Hex.encodeHexString(payloadBytes)), e);
            return Optional.empty();
        }
    }

    private static boolean typeHasSubtype(final byte type) {
        return type == 0x05;
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

    private static final int BYTE_SIZE_IN_BYTES = 1;
    private static final int INTEGER_SIZE_IN_BYTES = Integer.SIZE / 8;
    private static final int HEADER_SIZE_IN_BYTES = INTEGER_SIZE_IN_BYTES + 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationMessage.class);
}
