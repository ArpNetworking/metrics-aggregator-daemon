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

import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.sources.KafkaSource;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Tests to check integration with a kafka topic.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class KafkaIT {
    private static final String HOST = "localhost";
    private static final String PORT = "9092";
    private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.IntegerSerializer";
    private static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private Map<String, Object> _consumerProps;
    private Map<String, Object> _producerProps;
    private static final int POLL_DURATION_MILLIS = 1000;
    private static final int TIMEOUT = 5000;

    @Before
    public void setUp() {
        _producerProps = Maps.newHashMap();
        _producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        _producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + PORT);
        _producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "producer0");
        _producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
        _producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
        _producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id");

        _consumerProps = Maps.newHashMap();
        _consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + PORT);
        _consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        _consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        _consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        _consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void testKafkaSourceSingleObserver() {
        // Create kafka topic
        final String topicName = createTopicName();
        createTopic(topicName);

        // Create and send producer records
        final List<ProducerRecord<Integer, String>> producerRecords = createProducerRecords(topicName, 3);
        sendRecords(producerRecords);

        // Create consumer
        _consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + topicName);
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(_consumerProps);
        consumer.subscribe(Collections.singletonList(topicName));

        // Create Kafka Source
        final Observer observer = Mockito.mock(Observer.class);
        final KafkaSource<String> source = new KafkaSource.Builder<String>()
                .setName("KafkaSource")
                .setConsumer(consumer)
                .setPollTimeMillis(POLL_DURATION_MILLIS)
                .build();

        // Observe records
        source.attach(observer);
        source.start();
        for (ProducerRecord<Integer, String> expected : producerRecords) {
            Mockito.verify(observer, Mockito.timeout(TIMEOUT)).notify(source, expected.value());
        }
        source.stop();
    }

    @Test
    public void testKafkaSourceMultipleObservers() {
        // Create kafka topic
        final String topicName = createTopicName();
        createTopic(topicName);

        // Create and send producer records
        final List<ProducerRecord<Integer, String>> producerRecords = createProducerRecords(topicName, 3);
        sendRecords(producerRecords);

        // Create consumer
        _consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + topicName);
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(_consumerProps);
        consumer.subscribe(Collections.singletonList(topicName));

        // Create Kafka Source
        final Observer observer1 = Mockito.mock(Observer.class);
        final Observer observer2 = Mockito.mock(Observer.class);
        final KafkaSource<String> source = new KafkaSource.Builder<String>()
                .setName("KafkaSource")
                .setConsumer(consumer)
                .setPollTimeMillis(POLL_DURATION_MILLIS)
                .build();

        // Observe records
        source.attach(observer1);
        source.attach(observer2);
        source.start();
        for (ProducerRecord<Integer, String> expected : producerRecords) {
            Mockito.verify(observer1, Mockito.timeout(TIMEOUT)).notify(source, expected.value());
            Mockito.verify(observer2, Mockito.timeout(TIMEOUT)).notify(source, expected.value());
        }
        source.stop();
    }

    private String createTopicName() {
        return "topic_" + UUID.randomUUID().toString();
    }

    private void createTopic(final String topicName) {
        try {
            final Properties config = new Properties();
            config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + PORT);
            config.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "adminClient");
            final AdminClient adminClient = AdminClient.create(config);
            final NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

            // Create topic, which is async call
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

            // Wait for it to complete
            createTopicsResult.values().get(topicName).get();
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e.getMessage(), e);
            }
            // Ignore TopicExistsException
        }
    }

    private List<ProducerRecord<Integer, String>> createProducerRecords(final String topic, final int num) {
        final List<ProducerRecord<Integer, String>> records = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            records.add(new ProducerRecord<>(topic, i, "value" + i));
        }
        return records;
    }

    private void sendRecords(final List<ProducerRecord<Integer, String>> records) {
        final KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(_producerProps);
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (ProducerRecord<Integer, String> record : records) {
                producer.send(record);
            }
            producer.commitTransaction();
        } catch (final ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            producer.close();
        } catch (final KafkaException e) {
            producer.abortTransaction();
        }
        producer.close();
    }
}
