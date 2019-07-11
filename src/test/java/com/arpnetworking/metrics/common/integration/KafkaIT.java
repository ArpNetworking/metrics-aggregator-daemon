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
package com.arpnetworking.metrics.common.integration;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.kafka.ConsumerDeserializer;
import com.arpnetworking.metrics.common.sources.KafkaSource;
import com.arpnetworking.test.StringParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests to check integration with a kafka topic.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class KafkaIT {
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.IntegerSerializer";
    private static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final Duration POLL_DURATION = Duration.ofSeconds(1);
    private static final int TIMEOUT = 5000;

    private Map<String, Object> _consumerProps;
    private KafkaConsumer<Integer, String> _consumer;
    private String _topicName;
    private KafkaSource<String, String> _source;
    private List<ProducerRecord<Integer, String>> _producerRecords;

    @Before
    public void setUp() throws TimeoutException {
        // Create kafka topic
        _topicName = createTopicName();
        createTopic(_topicName);

        // Create and send producer records
        _producerRecords = createProducerRecords(_topicName, 3);
        sendRecords(_producerRecords);

        // Create consumer props
        _consumerProps = Maps.newHashMap();
        _consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        _consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        _consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        _consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        _consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        _consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + _topicName);
    }

    @After
    public void tearDown() {
        if (_source != null) {
            _source.stop();
        }
    }

    @Test
    public void testKafkaSourceSingleObserver() {
        // Create Kafka Source
        _consumer = new KafkaConsumer<>(_consumerProps);
        _consumer.subscribe(Collections.singletonList(_topicName));
        _source = new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setParser(new StringParser())
                .setConsumer(_consumer)
                .setPollTime(POLL_DURATION)
                .build();

        // Observe records
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();
        for (ProducerRecord<Integer, String> expected : _producerRecords) {
            Mockito.verify(observer, Mockito.timeout(TIMEOUT)).notify(_source, expected.value());
        }
    }

    @Test
    public void testKafkaSourceMultipleObservers() {
        // Create Kafka Source
        _consumer = new KafkaConsumer<>(_consumerProps);
        _consumer.subscribe(Collections.singletonList(_topicName));
        _source = new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setParser(new StringParser())
                .setConsumer(_consumer)
                .setPollTime(POLL_DURATION)
                .build();

        // Observe records
        final Observer observer1 = Mockito.mock(Observer.class);
        final Observer observer2 = Mockito.mock(Observer.class);
        _source.attach(observer1);
        _source.attach(observer2);
        _source.start();
        for (ProducerRecord<Integer, String> expected : _producerRecords) {
            Mockito.verify(observer1, Mockito.timeout(TIMEOUT)).notify(_source, expected.value());
            Mockito.verify(observer2, Mockito.timeout(TIMEOUT)).notify(_source, expected.value());
        }
    }

    @Test
    public void testKafkaSourceFromConfig() throws IOException {
        final String jsonString =
                    "{"
                + "\n  \"type\":\"com.arpnetworking.metrics.common.sources.KafkaSource\","
                + "\n  \"name\":\"kafka_source\","
                + "\n  \"pollTime\":\"PT1S\","
                + "\n  \"consumer\":{"
                + "\n    \"type\":\"org.apache.kafka.clients.consumer.Consumer\","
                + "\n    \"topics\":[\"" + _topicName + "\"],"
                + "\n    \"configs\":{"
                + "\n      \"bootstrap.servers\":\"" + KAFKA_SERVER + "\","
                + "\n      \"group.id\":\"group" + _topicName + "\","
                + "\n      \"client.id\":\"consumer0\","
                + "\n      \"key.deserializer\":\"" + KEY_DESERIALIZER + "\","
                + "\n      \"value.deserializer\":\"" + VALUE_DESERIALIZER + "\","
                + "\n      \"auto.offset.reset\":\"earliest\""
                + "\n    }"
                + "\n  },"
                + "\n  \"parser\":{"
                + "\n    \"type\":\"com.arpnetworking.test.StringParser\""
                + "\n  },"
                + "\n  \"shutdownAwaitTime\":\"PT10S\","
                + "\n  \"backoffTime\":\"PT1S\""
                + "\n}";

        final ObjectMapper mapper = ObjectMapperFactory.createInstance();
        final SimpleModule module = new SimpleModule("KafkaConsumer");
        module.addDeserializer(Consumer.class, new ConsumerDeserializer<>());
        mapper.registerModule(module);
        _source = mapper.readValue(jsonString, new KafkaSourceStringType());

        // Observe records
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();
        for (ProducerRecord<Integer, String> expected : _producerRecords) {
            Mockito.verify(observer, Mockito.timeout(TIMEOUT)).notify(_source, expected.value());
        }
    }

    private String createTopicName() {
        return "topic_" + UUID.randomUUID().toString();
    }

    private static void createTopic(final String topicName) throws TimeoutException {
        try {
            final Properties config = new Properties();
            config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
            config.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "adminClient");
            final AdminClient adminClient = AdminClient.create(config);
            final NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

            // Create topic, which is async call
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

            // Wait for it to complete
            createTopicsResult.values().get(topicName).get(20, TimeUnit.SECONDS);
            adminClient.close();
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e.getMessage(), e);
            }
            // Ignore TopicExistsException
        }
    }

    private static List<ProducerRecord<Integer, String>> createProducerRecords(final String topic, final int num) {
        final List<ProducerRecord<Integer, String>> records = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            records.add(new ProducerRecord<>(topic, i, "value" + i));
        }
        return records;
    }

    private void sendRecords(final List<ProducerRecord<Integer, String>> records) {
        final Map<String, Object> producerProps;
        producerProps = Maps.newHashMap();
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "producer0");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id");

        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (ProducerRecord<Integer, String> record : records) {
                producer.send(record);
            }
            producer.commitTransaction();
        } catch (final ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            producer.close();
            Assert.fail("Failed sending records to kafka server");
        } catch (final KafkaException e) {
            producer.abortTransaction();
            producer.close();
            Assert.fail("Failed sending records to kafka server");
        }
        producer.close();
    }

    /**
     * Class needed for generic object mapping with {@code ObjectMapper}.
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    private static class KafkaSourceStringType extends TypeReference<KafkaSource<String, String>> {}
}
