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
package com.arpnetworking.metrics.common.sources;

import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.steno.LogBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.test.CollectingPeriodicMetrics;
import com.arpnetworking.test.StringParser;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Unit tests for the {@link KafkaSource} class.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class KafkaSourceTest {

    private static final List<String> EXPECTED = createValues("value", 300);
    private static final String TOPIC = "test_topic";
    private static final int PARTITION = 0;
    private static final Duration POLL_DURATION = Duration.ofSeconds(1);
    private static final int TIMEOUT = 5000;

    private KafkaSource<String, String> _source;
    private Logger _logger;
    private LogBuilder _logBuilder;

    private CollectingPeriodicMetrics _periodicMetrics;
    private ScheduledExecutorService _executor;

    @Before
    public void setUp() {
        _logger = Mockito.mock(Logger.class);
        _logBuilder = Mockito.mock(LogBuilder.class);
        Mockito.when(_logger.trace()).thenReturn(_logBuilder);
        Mockito.when(_logger.debug()).thenReturn(_logBuilder);
        Mockito.when(_logger.info()).thenReturn(_logBuilder);
        Mockito.when(_logger.warn()).thenReturn(_logBuilder);
        Mockito.when(_logger.error()).thenReturn(_logBuilder);
        Mockito.when(_logBuilder.setMessage(Mockito.anyString())).thenReturn(_logBuilder);
        Mockito.when(_logBuilder.addData(Mockito.anyString(), Mockito.any())).thenReturn(_logBuilder);
        Mockito.when(_logBuilder.addContext(Mockito.anyString(), Mockito.any())).thenReturn(_logBuilder);
        Mockito.when(_logBuilder.setEvent(Mockito.anyString())).thenReturn(_logBuilder);
        Mockito.when(_logBuilder.setThrowable(Mockito.any(Throwable.class))).thenReturn(_logBuilder);


        _periodicMetrics = new CollectingPeriodicMetrics();
        _executor = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "PeriodicMetricsCloser"));
        _executor.scheduleAtFixedRate(_periodicMetrics, 500, 500, TimeUnit.MILLISECONDS);
    }

    @After
    public void tearDown() {
        _executor.shutdown();
    }

    @Test
    public void testSourceSingleWorkerSuccess() {
        createHealthySource(1);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        for (String expected : EXPECTED) {
            Mockito.verify(observer, Mockito.timeout(TIMEOUT)).notify(_source, expected);
        }
        _source.stop();

        // Check metrics
        Assert.assertEquals(EXPECTED.size(), _periodicMetrics.getCounters().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    public void testSourceMultiWorkerSuccess() {
        createHealthySource(4);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(EXPECTED.size())).notify(Mockito.any(), captor.capture());
        Assert.assertEquals(
                EXPECTED.stream().sorted().collect(Collectors.toList()),
                captor.getAllValues().stream().sorted().collect(Collectors.toList())
        );
        _source.stop();

        // Check metrics
        Assert.assertEquals(EXPECTED.size(), _periodicMetrics.getCounters().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    public void testSourceMultiWorkerFillQueue() {
        createFillingQueueSource();
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(EXPECTED.size())).notify(Mockito.any(),
                captor.capture());
        Assert.assertEquals(
                EXPECTED.stream().sorted().collect(Collectors.toList()),
                captor.getAllValues().stream().sorted().collect(Collectors.toList())
        );
        _source.stop();

        // Check metrics
        Assert.assertEquals(EXPECTED.size(), _periodicMetrics.getCounters().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    public void testSourceKafkaException() {
        createExceptionSource(KafkaException.class);
        _source.start();
        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeastOnce())
                .setMessage("Consumer received Kafka Exception");
        _source.stop();
    }

    @Test
    public void testSourceRuntimeException() {
        createExceptionSource(RuntimeException.class);
        _source.start();
        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeastOnce()).setMessage("Consumer thread error");
        _source.stop();
    }

    @Test
    public void testSourceParsingException() throws ParsingException {
        createBadParsingSource();
        _source.start();
        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeastOnce()).setMessage("Failed to parse data");
        _source.stop();
    }

    private void createFillingQueueSource() {
        _source = new KafkaSource<>(new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(createMockConsumer())
                .setParser(new StringParser())
                .setPollTime(POLL_DURATION)
                .setPeriodicMetrics(_periodicMetrics)
                .setNumWorkerThreads(1),
                new FillingBlockingQueue(100));
    }

    private void createHealthySource(final int numWorkers) {
        _source = new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(createMockConsumer())
                .setParser(new StringParser())
                .setPollTime(POLL_DURATION)
                .setPeriodicMetrics(_periodicMetrics)
                .setNumWorkerThreads(numWorkers)
                .build();
    }

    private void createExceptionSource(final Class<? extends Exception> exception) {
        final Consumer<String, String> consumer = Mockito.mock(ConsumerSS.class);
        final Map<TopicPartition, List<ConsumerRecord<String, String>>> records = Maps.newHashMap();
        records.put(new TopicPartition(TOPIC, PARTITION), Collections.singletonList(
                new ConsumerRecord<>(TOPIC, PARTITION, 0, "0", "value0")));
        Mockito.when(consumer.poll(Mockito.any()))
                .thenReturn(new ConsumerRecords<>(records))
                .thenThrow(exception);
        _source = new KafkaSource<>(new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(consumer)
                .setParser(new StringParser())
                .setPeriodicMetrics(_periodicMetrics)
                .setPollTime(POLL_DURATION),
                _logger);
    }

    private void createBadParsingSource() throws ParsingException {
        final Parser<String, String> parser = Mockito.mock(StringParser.class);
        Mockito.when(parser.parse(Mockito.anyString()))
                .thenThrow(new ParsingException("Could not parse data", "bad_data".getBytes(Charsets.UTF_8)));

        _source = new KafkaSource<>(new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(createMockConsumer())
                .setParser(parser)
                .setPeriodicMetrics(_periodicMetrics)
                .setPollTime(POLL_DURATION),
                _logger);
    }

    private static List<String> createValues(final String prefix, final int num) {
        final List<String> values = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            values.add(prefix + i);
        }
        return values;
    }

    private static MockConsumer<String, String> createMockConsumer() {
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC, PARTITION)));
        long offset = 0L;
        final Map<TopicPartition, Long> beginningOffsets = Maps.newHashMap();
        beginningOffsets.put(new TopicPartition(TOPIC, PARTITION), offset);
        consumer.updateBeginningOffsets(beginningOffsets);

        for (final String value : EXPECTED) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, offset++, "" + offset, value));
        }

        return consumer;
    }

    /**
     * Interface needed to mock generic interface.
     */
    private interface ConsumerSS extends Consumer<String, String> {}

    private static class FillingBlockingQueue extends ArrayBlockingQueue<String> {
        private AtomicBoolean _enabled = new AtomicBoolean(false);
        private static final long serialVersionUID = 1L;

        FillingBlockingQueue(final int capacity) {
            super(capacity);
        }

        @Override
        public String poll() {
            return _enabled.get() ? super.poll() : null;
        }

        @Override
        public void put(final String element) throws InterruptedException {
            if (remainingCapacity() == 0) {
                _enabled.set(true);
            }
            super.put(element);
        }
    }
}
