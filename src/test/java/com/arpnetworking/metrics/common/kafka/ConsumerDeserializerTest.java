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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Unit Tests for <code>ConsumerDeserializer</code>.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class ConsumerDeserializerTest {

    private static final String VALID_CONFIGS = "\"configs\": {"
            + "\"group.id\":\"group0\","
            + "\"bootstrap.servers\":\"localhost:9092\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\""
            + "}";
    private static final String VALID_TOPICS = "\"topics\":[\"test\"]";

    /*
     *  {
     *      "configs": {
     *          "group.id":"group0",
     *          "bootstrap.servers":"localhost:9092",
     *          "key.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
     *          "value.deserializer":"org.apache.kafka.common.serialization.StringDeserializer"
     *      },
     *      "topics":["test"]
     *  }
     */
    private static final String CONSUMER_JSON_VALID = makeJsonObject(VALID_CONFIGS, VALID_TOPICS);
    private static final String CONSUMER_JSON_MALFORMED = "{" + VALID_CONFIGS + VALID_TOPICS + "}";
    private static final String CONSUMER_JSON_NO_KEY_DESERIALIZER = makeJsonObject(
            "\"configs\": {\"group.id\":\"group0\",\"bootstrap.servers\":\"localhost:9092\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\"}", VALID_TOPICS);
    private static final String CONSUMER_JSON_NO_VALUE_DESERIALIZER = makeJsonObject(
            "\"configs\": {\"group.id\":\"group0\",\"bootstrap.servers\":\"localhost:9092\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\"}", VALID_TOPICS);
    private static final String CONSUMER_JSON_NO_BOOTSTRAP_SERVER = makeJsonObject(
            "\"configs\": {\"group.id\":\"group0\","
            + "\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
            + "\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\"}", VALID_TOPICS);
    private static final String CONSUMER_JSON_NO_CONFIG = makeJsonObject(VALID_TOPICS);
    private static final String CONSUMER_JSON_NO_TOPICS = makeJsonObject(VALID_CONFIGS);
    private static final String CONSUMER_JSON_CONFIGS_NOT_OBJECT = makeJsonObject("\"configs\": true", VALID_TOPICS);
    private static final String CONSUMER_JSON_CONFIGS_NULL = makeJsonObject("\"configs\": null", VALID_TOPICS);
    private static final String CONSUMER_JSON_TOPICS_NOT_LIST = makeJsonObject(VALID_CONFIGS, "\"topics\":\"test\"");
    private static final String CONSUMER_JSON_TOPICS_NULL = makeJsonObject(VALID_CONFIGS, "\"topics\":null");

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();

    @BeforeClass
    public static void setUpClass() throws IOException {
        Files.createDirectories(new File("./target/tmp/filter/ConsumerDeserializer").toPath());
        final SimpleModule module = new SimpleModule("KafkaConsumer");
        module.addDeserializer(Consumer.class, new ConsumerDeserializer<>());
        OBJECT_MAPPER.registerModule(module);
    }

    /* --- Success --- */
    @Test
    public void testDeserializeConsumerSuccess() throws IOException {
        final Consumer<String, String> consumer = createConsumerFromJSON(CONSUMER_JSON_VALID);
        Assert.assertEquals(Sets.newHashSet("test"), consumer.subscription());
    }

    /* --- Invalid JSON --- */
    @Test(expected = JsonParseException.class)
    public void testDeserializeConsumerMalformedJson() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_MALFORMED);
    }

    /* --- Not JSON Object --- */
    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerNotJsonObjectNumber() throws IOException {
        createConsumerFromJSON("1000");
    }

    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerNotJsonObjectList() throws IOException {
        createConsumerFromJSON("[10, 10]");
    }

    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerNotJsonObjectBoolean() throws IOException {
        createConsumerFromJSON("true");
    }

    /* --- Valid JSON Object but Wrong Field Types--- */
    @Test(expected = JsonMappingException.class)
    public void testDeserializeConsumerTopicsNotList() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_TOPICS_NOT_LIST);
    }

    @Test(expected = JsonMappingException.class)
    public void testDeserializeConsumerConfigsNotObject() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_CONFIGS_NOT_OBJECT);
    }

    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerTopicsNotListNull() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_TOPICS_NULL);
    }

    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerConfigsNotObjectNull() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_CONFIGS_NULL);
    }

    /* --- Valid JSON Object but Missing Fields --- */
    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerMissingConfigsField() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_CONFIG);
    }

    @Test(expected = MismatchedInputException.class)
    public void testDeserializeConsumerMissingTopicsField() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_TOPICS);
    }

    /* --- Valid JSON but Configs Missing Needed Fields --- */
    @Test(expected = JsonMappingException.class)
    public void testDeserializeConsumerMissingKeyDeserializer() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_KEY_DESERIALIZER);
    }

    @Test(expected = JsonMappingException.class)
    public void testDeserializeConsumerMissingValueDeserializer() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_VALUE_DESERIALIZER);
    }

    @Test(expected = JsonMappingException.class)
    public void testDeserializeConsumerMissingBootstrapServer() throws IOException {
        createConsumerFromJSON(CONSUMER_JSON_NO_BOOTSTRAP_SERVER);
    }

    private static Consumer<String, String> createConsumerFromJSON(final String jsonString)
            throws IOException {
        return OBJECT_MAPPER.readValue(jsonString, new TypeReference<Consumer<String, String>>() {});
    }

    private static String makeJsonObject(final String... fields) {
        final StringBuilder jsonObject = new StringBuilder("{");
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                jsonObject.append(",");
            }
            jsonObject.append(fields[i]);
        }
        jsonObject.append("}");
        return jsonObject.toString();
    }
}
