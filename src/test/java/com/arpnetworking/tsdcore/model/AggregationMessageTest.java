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

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.UnknownFieldSet;
import io.vertx.core.buffer.Buffer;
import org.apache.pekko.util.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteOrder;

/**
 * Tests for the AggregationMessage class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class AggregationMessageTest {

    @Test
    public void testToBufferHostIdentification() {
        final GeneratedMessage protobufMessage = Messages.HostIdentification.getDefaultInstance();
        final AggregationMessage message = AggregationMessage.create(protobufMessage);
        Assert.assertNotNull(message);
        Assert.assertSame(protobufMessage, message.getMessage());

        final Buffer vertxBuffer = message.serializeToBuffer();
        final byte[] messageBuffer = vertxBuffer.getBytes();
        final byte[] protobufBuffer = protobufMessage.toByteArray();

        // Assert length
        Assert.assertEquals(protobufBuffer.length + 5, messageBuffer.length);
        Assert.assertEquals(protobufBuffer.length + 5, vertxBuffer.getInt(0));
        Assert.assertEquals(protobufBuffer.length + 5, message.getLength());

        // Assert payload type
        Assert.assertEquals(1, messageBuffer[4]);

        // Assert the payload was not corrupted
        for (int i = 0; i < protobufBuffer.length; ++i) {
            Assert.assertEquals(protobufBuffer[i], messageBuffer[i + 5]);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToBufferSerializedUnsupportedMessage() {
        final GeneratedMessage mockMessage = Mockito.mock(GeneratedMessage.class);
        Mockito.doReturn(UnknownFieldSet.getDefaultInstance()).when(mockMessage).getUnknownFields();
        AggregationMessage.create(mockMessage).serializeToBuffer();
    }

    @Test
    public void testToByteStringHostIdentification() {
        final GeneratedMessage protobufMessage = Messages.HostIdentification.getDefaultInstance();
        final AggregationMessage message = AggregationMessage.create(protobufMessage);
        Assert.assertNotNull(message);
        Assert.assertSame(protobufMessage, message.getMessage());

        final ByteString pekkoByteString = message.serializeToByteString();
        final byte[] messageBuffer = pekkoByteString.toArray();
        final byte[] protobufBuffer = protobufMessage.toByteArray();

        // Assert length
        Assert.assertEquals(protobufBuffer.length + 5, messageBuffer.length);
        Assert.assertEquals(protobufBuffer.length + 5, pekkoByteString.iterator().getInt(ByteOrder.BIG_ENDIAN));
        Assert.assertEquals(protobufBuffer.length + 5, message.getLength());

        // Assert payload type
        Assert.assertEquals(1, messageBuffer[4]);

        // Assert the payload was not corrupted
        for (int i = 0; i < protobufBuffer.length; ++i) {
            Assert.assertEquals(protobufBuffer[i], messageBuffer[i + 5]);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToByteStringSerializedUnsupportedMessage() {
        final GeneratedMessage mockMessage = Mockito.mock(GeneratedMessage.class);
        Mockito.doReturn(UnknownFieldSet.getDefaultInstance()).when(mockMessage).getUnknownFields();
        AggregationMessage.create(mockMessage).serializeToByteString();
    }

    @Test
    public void testPekkoVsVertx() {
        final GeneratedMessage protobufMessage = Messages.HostIdentification.getDefaultInstance();
        final AggregationMessage message = AggregationMessage.create(protobufMessage);
        Assert.assertNotNull(message);
        Assert.assertSame(protobufMessage, message.getMessage());

        final ByteString pekkoByteString = message.serializeToByteString();
        final Buffer vertxBuffer = message.serializeToBuffer();

        final byte[] pekkoBytes = pekkoByteString.toArray();
        final byte[] vertxBytes = vertxBuffer.getBytes();

        // Assert length
        Assert.assertEquals(pekkoBytes.length, vertxBytes.length);
        Assert.assertArrayEquals(pekkoBytes, vertxBytes);
    }
}
