/*
 * Copyright 2025 Brandon Arp
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
package com.arpnetworking.configuration.jackson.module.pekko;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.pekko.actor.TimerSchedulerImpl;

import java.io.IOException;

/**
 * Serializer for a Pekko timer message.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class TimerMessageSerializer extends JsonSerializer<TimerSchedulerImpl.TimerMsg> {
    @Override
    public void serialize(
            final TimerSchedulerImpl.TimerMsg timerMsg,
            final JsonGenerator jsonGenerator,
            final SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("type", timerMsg.getClass().getTypeName());
        jsonGenerator.writeObjectField("key", timerMsg.key());
        jsonGenerator.writeObjectField("generation", timerMsg.generation());
        jsonGenerator.writeObjectField("owner", timerMsg.owner());
        jsonGenerator.writeEndObject();
    }
}
