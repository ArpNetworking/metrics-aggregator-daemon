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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Jackson <code>JsonDeserializer</code> implementation for <code>Consumer</code>.
 *
 * @param <K> the type of key field in <code>Consumer</code>
 * @param <V> the type of value field in <code>Consumer</code>
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class ConsumerDeserializer<K, V> extends JsonDeserializer<Consumer<K, V>> {

    @Override
    public Consumer<K, V> deserialize(final JsonParser parser, final DeserializationContext context)
            throws IOException, JsonProcessingException {
        // Parse input json into JsonNode Tree
        final JsonDeserializer<? extends JsonNode> deserializer = JsonNodeDeserializer.getDeserializer(ObjectNode.class);
        final JsonNode node = deserializer.deserialize(parser, context);

        // Pull out configs and topics fields and convert deserialize with standard mapper
        final JsonNode configNode = node.get("configs");
        final JsonNode topicsNode = node.get("topics");
        if (configNode == null) {
            throw MismatchedInputException.from(parser, Consumer.class,
                    "Consumer object missing \"configs\" field");
        }
        if (topicsNode == null) {
            throw MismatchedInputException.from(parser, Consumer.class,
                    "Consumer object missing \"topics\" field");
        }

        final ObjectMapper mapper = ObjectMapperFactory.getInstance();

        final TypeReference<Map<String, String>> configsType = new TypeReference<Map<String, String>>() {};
        final TypeReference<List<String>> topicsType = new TypeReference<List<String>>() {};
        final Map<String, Object> configs;
        final List<String> topics;

        try {
            configs = mapper.convertValue(configNode, configsType);
        } catch (final IllegalArgumentException e) {
            throw new JsonMappingException(parser, "\"configs\" field must be an object", e);
        }

        try {
            topics = mapper.convertValue(topicsNode, topicsType);
        } catch (final IllegalArgumentException e) {
            throw new JsonMappingException(parser, "\"topics\" field must be a list", e);
        }

        if (configs == null) {
            throw MismatchedInputException.from(parser, Consumer.class,
                    "\"configs\" field cannot have null value");
        }
        if (topics == null) {
            throw MismatchedInputException.from(parser, Consumer.class,
                    "\"topics\" field cannot have null value");
        }

        // Create consumer
        try {
            final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(configs);
            consumer.subscribe(topics);
            return consumer;
        } catch (final ConfigException e) {
            throw new JsonMappingException(parser, "Error in Kafka Consumer configuration", e);
        } catch (final KafkaException e) {
            throw new JsonMappingException(parser, "Error creating Kafka Consumer", e);
        }

    }
}
