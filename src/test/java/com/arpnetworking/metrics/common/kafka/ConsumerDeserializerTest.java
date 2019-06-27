/*
 * Copyright 2019 Dropbox.com
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
package com.arpnetworking.metrics.common.kafka;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

/**
 * Unit Tests for <code>ConsumerDeserializer</code>.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class ConsumerDeserializerTest {

    private static final String TOPIC = UUID.randomUUID().toString();
    /*
     *  {
     *      "configs": {
     *          "bootstrap.servers":"localhost:9092",
     *          "group.id":"test",
     *          "client.id":"consumer0"
     *          "key.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
     *          "value.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
     *          "auto.offset.reset":"earliest"
     *      },
     *      "topics":["configure-topic"]
     *  }
     */
    private static final String CONSUMER_JSON_VALID = "{\"configs\": {"
            + "\"bootstrap.servers\":\"localhost:9092\","
            + "\"group.id\":\"test\",\"client.id\":\"consumer0\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"auto.offset.reset\":\"earliest\""
            + "},\"topics\":[\"" + TOPIC + "\"]}";

    private static final String CONSUMER_JSON_MALFORMED = "{\"configs\": "  // Missing opening brace
            + "\"bootstrap.servers\":\"localhost:9092\","
            + "\"group.id\":\"test\",\"client.id\":\"consumer0\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"auto.offset.reset\":\"earliest\""
            + "},\"topics\":[\"" + TOPIC + "\"]}";

    private static final String CONSUMER_JSON_NO_KEY_DESERIALIZER = "{\"configs\": {"
            + "\"bootstrap.servers\":\"localhost:9092\","
            + "\"group.id\":\"test\",\"client.id\":\"consumer0\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"auto.offset.reset\":\"earliest\""
            + "},\"topics\":[\"" + TOPIC + "\"]}";

    private static final String CONSUMER_JSON_NO_VALUE_DESERIALIZER = "{\"configs\": {"
            + "\"bootstrap.servers\":\"localhost:9092\","
            + "\"group.id\":\"test\",\"client.id\":\"consumer0\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"auto.offset.reset\":\"earliest\""
            + "},\"topics\":[\"" + TOPIC + "\"]}";

    private static final String CONSUMER_JSON_NO_BOOTSTRAP_SERVER = "{\"configs\": {"
            + "\"group.id\":\"test\",\"client.id\":\"consumer0\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"auto.offset.reset\":\"earliest\""
            + "},\"topics\":[\"" + TOPIC + "\"]}";

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();

    @BeforeClass
    public static void setUpClass() throws IOException {
        Files.createDirectories(new File("./target/tmp/filter/ConsumerDeserializer").toPath());
        final SimpleModule module = new SimpleModule("KafkaConsumer");
        module.addDeserializer(Consumer.class, new ConsumerDeserializer<>());
        OBJECT_MAPPER.registerModule(module);
    }

    @Test
    public void testDeserializeConsumerSuccess() throws IOException {
        final Consumer<String, String> consumer = createConsumerFromJSON(CONSUMER_JSON_VALID, "validConsumer.json");
        Assert.assertEquals(Sets.newHashSet(TOPIC), consumer.subscription());
    }

    @Test(expected = JsonParseException.class)
    public void testDeserializeConsumerMalformedJson() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_MALFORMED, "malformedJson.json");
    }

    @Test(expected = ConfigException.class)
    public void testDeserializeConsumerMissingKeyDeserializer() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_KEY_DESERIALIZER, "keyDeserializerMissing.json");
    }

    @Test(expected = ConfigException.class)
    public void testDeserializeConsumerMissingValueDeserializer() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_VALUE_DESERIALIZER, "valueDeserializerMissing.json");
    }

    @Test(expected = KafkaException.class)
    public void testDeserializeConsumerMissingBootstrapServer() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_BOOTSTRAP_SERVER, "bootstrapServerMissing.json");
    }

    private static Consumer<String, String> createConsumerFromJSON(final String jsonString, final String fileName)
            throws IOException {
        final File file = new File("./target/tmp/filter/ConsumerDeserializer/" + fileName);
        Files.write(file.toPath(), jsonString.getBytes(Charsets.UTF_8));


        final Consumer<String, String> consumer = OBJECT_MAPPER.readValue(
                new File("./target/tmp/filter/ConsumerDeserializer/" + fileName),
                new TypeReference<Consumer<String, String>>() {});

        return consumer;
    }
}
