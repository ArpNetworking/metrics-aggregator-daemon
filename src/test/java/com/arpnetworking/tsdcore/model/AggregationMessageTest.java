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

import akka.util.ByteString;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.UnknownFieldSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.vertx.java.core.buffer.Buffer;

/**
 * Tests for the AggregationMessage class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class AggregationMessageTest {

    @Test
    public void testHostIdentification() {
        final GeneratedMessage protobufMessage = Messages.HostIdentification.getDefaultInstance();
        final AggregationMessage message = AggregationMessage.create(protobufMessage);
        Assert.assertNotNull(message);
        Assert.assertSame(protobufMessage, message.getMessage());

        final Buffer vertxBuffer = message.serialize();
        final byte[] messageBuffer = vertxBuffer.getBytes();
        final byte[] protobufBuffer = protobufMessage.toByteArray();
        ByteString.fromArray(vertxBuffer.getBytes());

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
    public void testSerializedUnsupportedMessage() {
        final GeneratedMessage mockMessage = Mockito.mock(GeneratedMessage.class);
        Mockito.doReturn(UnknownFieldSet.getDefaultInstance()).when(mockMessage).getUnknownFields();
        AggregationMessage.create(mockMessage).serialize();
    }
}
