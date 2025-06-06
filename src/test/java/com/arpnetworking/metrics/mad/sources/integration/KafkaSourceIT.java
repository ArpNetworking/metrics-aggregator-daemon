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
package com.arpnetworking.metrics.mad.sources.integration;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.kafka.ConsumerDeserializer;
import com.arpnetworking.metrics.common.sources.KafkaSource;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.test.StringToRecordParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice7.GuiceAnnotationIntrospector;
import com.fasterxml.jackson.module.guice7.GuiceInjectableValues;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
import org.mockito.ArgumentCaptor;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Tests to check integration with a kafka topic.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class KafkaSourceIT {
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.IntegerSerializer";
    private static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final Duration POLL_DURATION = Duration.ofSeconds(1);
    private static final int TIMEOUT = 10000;
    private static final int NUM_RECORDS = 500;
    private static final AtomicInteger RECORD_ID = new AtomicInteger(0);

    private Map<String, Object> _consumerProps;
    private KafkaConsumer<Integer, String> _consumer;
    private String _topicName;
    private KafkaSource<String> _source;
    private List<ProducerRecord<Integer, String>> _producerRecords;
    private PeriodicMetrics _periodicMetrics;

    @Before
    public void setUp() throws TimeoutException {
        // Create kafka topic
        _topicName = createTopicName();
        createTopic(_topicName);
        setupKafka();
        _periodicMetrics = Mockito.mock(PeriodicMetrics.class);
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
        _source = new KafkaSource.Builder<String>()
                .setName("KafkaSource")
                .setParser(new StringToRecordParser())
                .setConsumer(_consumer)
                .setPollTime(POLL_DURATION)
                .setPeriodicMetrics(_periodicMetrics)
                .build();

        // Observe records
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();
        for (ProducerRecord<Integer, String> expected : _producerRecords) {
            Mockito.verify(observer, Mockito.timeout(TIMEOUT)).notify(_source,
                    StringToRecordParser.recordWithID(expected.value()));
        }
    }

    @Test
    public void testKafkaSourceMultipleObservers() {
        // Create Kafka Source
        _consumer = new KafkaConsumer<>(_consumerProps);
        _consumer.subscribe(Collections.singletonList(_topicName));
        _source = new KafkaSource.Builder<String>()
                .setName("KafkaSource")
                .setParser(new StringToRecordParser())
                .setConsumer(_consumer)
                .setPollTime(POLL_DURATION)
                .setPeriodicMetrics(_periodicMetrics)
                .build();

        // Observe records
        final Observer observer1 = Mockito.mock(Observer.class);
        final Observer observer2 = Mockito.mock(Observer.class);
        _source.attach(observer1);
        _source.attach(observer2);
        _source.start();
        for (ProducerRecord<Integer, String> expected : _producerRecords) {
            Mockito.verify(observer1, Mockito.timeout(TIMEOUT)).notify(_source,
                    StringToRecordParser.recordWithID(expected.value()));
            Mockito.verify(observer2, Mockito.timeout(TIMEOUT)).notify(_source,
                    StringToRecordParser.recordWithID(expected.value()));
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
                + "\n    \"type\":\"com.arpnetworking.test.StringToRecordParser\""
                + "\n  },"
                + "\n  \"backoffTime\":\"PT1S\""
                + "\n}";

        final ObjectMapper mapper = ObjectMapperFactory.createInstance();

        final SimpleModule module = new SimpleModule("KafkaConsumer");
        module.addDeserializer(Consumer.class, new ConsumerDeserializer<>());
        mapper.registerModule(module);

        final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                super.configure();
                bind(PeriodicMetrics.class).toInstance(_periodicMetrics);
            }
        });
        mapper.setInjectableValues(new GuiceInjectableValues(injector));
        mapper.setAnnotationIntrospectors(
                new AnnotationIntrospectorPair(
                        guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()),
                new AnnotationIntrospectorPair(
                        guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector()));

        _source = mapper.readValue(jsonString, new KafkaSourceStringType());

        // Observe records
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();
        for (ProducerRecord<Integer, String> expected : _producerRecords) {
            Mockito.verify(observer, Mockito.timeout(TIMEOUT)).notify(_source,
                    StringToRecordParser.recordWithID(expected.value()));
        }
    }

    @Test
    public void testKafkaSourceMultipleWorkerThreads() {
        // Create Kafka Source
        _consumer = new KafkaConsumer<>(_consumerProps);
        _consumer.subscribe(Collections.singletonList(_topicName));
        _source = new KafkaSource.Builder<String>()
                .setName("KafkaSource")
                .setParser(new StringToRecordParser())
                .setConsumer(_consumer)
                .setPollTime(POLL_DURATION)
                .setNumWorkerThreads(4)
                .setPeriodicMetrics(_periodicMetrics)
                .build();

        // Observe records
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(NUM_RECORDS)).notify(Mockito.any(), captor.capture());
        Assert.assertEquals(
                _producerRecords.stream().map(ProducerRecord::value).sorted().collect(Collectors.toList()),
                captor.getAllValues().stream().map(StringToRecordParser::recordID).sorted().collect(Collectors.toList())
        );
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
            final int id = RECORD_ID.getAndIncrement();
            records.add(new ProducerRecord<>(topic, id, "value" + id));
        }
        return records;
    }

    private void sendRecords(final List<ProducerRecord<Integer, String>> records) {
        final Map<String, Object> producerProps;
        producerProps = Maps.newHashMap();
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "producer_" + _topicName);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-" + _topicName);

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

    private void setupKafka() {
        // Create and send producer records
        _producerRecords = createProducerRecords(_topicName, NUM_RECORDS);
        sendRecords(_producerRecords);

        // Create consumer props
        _consumerProps = Maps.newHashMap();
        _consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        _consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_" + _topicName);
        _consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        _consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        _consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        _consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + _topicName);
    }

    /**
     * Class needed for generic object mapping with {@code ObjectMapper}.
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    private static final class KafkaSourceStringType extends TypeReference<KafkaSource<String>> {}
}
