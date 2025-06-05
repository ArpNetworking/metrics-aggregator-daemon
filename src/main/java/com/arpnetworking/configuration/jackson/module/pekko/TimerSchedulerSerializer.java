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
import java.io.InvalidObjectException;
import java.lang.reflect.Field;

/**
 * Serializer for a Pekko timer message.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class TimerSchedulerSerializer extends JsonSerializer<TimerSchedulerImpl> {
    @Override
    public void serialize(
            final TimerSchedulerImpl timerScheduler,
            final JsonGenerator jsonGenerator,
            final SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("type", timerScheduler.getClass().getTypeName());
        try {
            jsonGenerator.writeObjectField("key", CTX.get(timerScheduler));
        } catch (final IllegalAccessException e) {
            throw new InvalidObjectException("Unable to access context field");
        }
        jsonGenerator.writeEndObject();
    }
    private static final Field CTX;
    static {
        try {
            CTX = TimerSchedulerImpl.class.getDeclaredField("ctx");
            CTX.setAccessible(true);
        } catch (final NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
