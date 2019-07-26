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
import com.arpnetworking.test.CollectorPeriodicMetrics;
import com.arpnetworking.test.StringParser;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
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
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private CollectorPeriodicMetrics _periodicMetrics;
    private ScheduledExecutorService _executor;
    private final AtomicInteger _exceptionsCount = new AtomicInteger(0);

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
        Mockito.when(_logBuilder.setThrowable(Mockito.any(Throwable.class))).thenAnswer(
                (Answer<LogBuilder>) invocation -> {
                    _exceptionsCount.getAndIncrement();
                    return _logBuilder;
                }
        );

        _periodicMetrics = Mockito.spy(new CollectorPeriodicMetrics());
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
        assertMetricCountsCorrect(EXPECTED.size(), EXPECTED.size(), 0, 0, 0);
    }

    @Test
    public void testSourceMultiWorkerSuccess() {
        createHealthySource(4);
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
        assertMetricCountsCorrect(EXPECTED.size(), EXPECTED.size(), 0, 0, 0);
    }

    @Test
    public void testSourceMultiWorkerFillQueue() {
        final int bufSize = 100;
        createFillingQueueSource(bufSize);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        // Check queue size gauge was set when queue is full
        Mockito.verify(_periodicMetrics,
                Mockito.timeout(TIMEOUT).atLeastOnce()).recordGauge(_source._queueSizeGaugeMetricName, (long) bufSize);

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(EXPECTED.size())).notify(Mockito.any(),
                captor.capture());
        Assert.assertEquals(
                EXPECTED.stream().sorted().collect(Collectors.toList()),
                captor.getAllValues().stream().sorted().collect(Collectors.toList())
        );

        _source.stop();
        assertMetricCountsCorrect(EXPECTED.size(), EXPECTED.size(), 0, 0, 0);
    }

    @Test
    public void testSourceKafkaExceptionBackoffFail() {
        final Exception exception = new KafkaException();
        createExceptionSource(exception, false);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeast(2)).setThrowable(exception);

        _source.stop();
        assertMetricCountsCorrect(0, 0, 0, _exceptionsCount.get(), 0);
    }

    @Test
    public void testSourceKafkaExceptionBackoffSuccess() {
        final Exception exception = new KafkaException();
        createExceptionSource(exception, true);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeastOnce()).setThrowable(exception);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(EXPECTED.size())).notify(Mockito.any(), Mockito.any());

        _source.stop();
        assertMetricCountsCorrect(EXPECTED.size(), EXPECTED.size(), 0, 1, 0);
    }

    @Test
    public void testSourceRuntimeExceptionBackoffFail() {
        final Exception exception = new RuntimeException();
        createExceptionSource(exception, false);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeast(2)).setThrowable(exception);

        _source.stop();
        assertMetricCountsCorrect(0, 0, 0, 0, _exceptionsCount.get());
    }

    @Test
    public void testSourceRuntimeExceptionBackoffSuccess() {
        final Exception exception = new RuntimeException();
        createExceptionSource(exception, true);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeastOnce()).setThrowable(exception);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(EXPECTED.size())).notify(Mockito.any(), Mockito.any());

        _source.stop();
        assertMetricCountsCorrect(EXPECTED.size(), EXPECTED.size(), 0, 0, 1);
    }

    @Test
    public void testSourceParsingException() throws ParsingException {
        final ParsingException exception =
                new ParsingException("Could not parse data", "bad_data".getBytes(Charsets.UTF_8));
        createBadParsingSource(exception);
        final Observer observer = Mockito.mock(Observer.class);
        _source.attach(observer);
        _source.start();

        Mockito.verify(_logBuilder, Mockito.timeout(TIMEOUT).atLeastOnce()).setThrowable(exception);
        Mockito.verify(observer, Mockito.timeout(TIMEOUT).times(EXPECTED.size() - 1)).notify(Mockito.any(),
                Mockito.any());

        _source.stop();
        assertMetricCountsCorrect(EXPECTED.size(), EXPECTED.size() - 1, 1, 0, 0);

    }

    private void createHealthySource(final int numWorkers) {
        _source = new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(createMockConsumer(expectedConsumerRecords()))
                .setParser(new StringParser())
                .setPollTime(POLL_DURATION)
                .setNumWorkerThreads(numWorkers)
                .setPeriodicMetrics(_periodicMetrics)
                .build();
    }

    private void createFillingQueueSource(final int bufferSize) {
        _source = new KafkaSource<>(new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(createMockConsumer(expectedConsumerRecords()))
                .setParser(new StringParser())
                .setPollTime(POLL_DURATION)
                .setNumWorkerThreads(1)
                .setPeriodicMetrics(_periodicMetrics),
                new FillingBlockingQueue(bufferSize));
    }

    private void createExceptionSource(final Exception exception, final boolean onlyOnce) {
        final Consumer<String, String> consumer = Mockito.spy(createMockConsumer(ConsumerRecords.empty()));

        if (onlyOnce) {
            Mockito.when(consumer.poll(Mockito.any()))
                    .thenThrow(exception)
                    .thenReturn(expectedConsumerRecords())
                    .thenCallRealMethod();
        } else {
            Mockito.when(consumer.poll(Mockito.any()))
                    .thenThrow(exception);
        }

        _source = new KafkaSource<>(new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(consumer)
                .setParser(new StringParser())
                .setPollTime(POLL_DURATION)
                .setPeriodicMetrics(_periodicMetrics)
                .setBackoffTime(Duration.ofMillis(10)),
                _logger);
    }

    private void createBadParsingSource(final ParsingException exception) throws ParsingException {
        final Parser<String, String> parser = Mockito.spy(new StringParser());
        Mockito.when(parser.parse(Mockito.anyString()))
                .thenThrow(exception)
                .thenCallRealMethod();

        _source = new KafkaSource<>(new KafkaSource.Builder<String, String>()
                .setName("KafkaSource")
                .setConsumer(createMockConsumer(expectedConsumerRecords()))
                .setParser(parser)
                .setPollTime(POLL_DURATION)
                .setPeriodicMetrics(_periodicMetrics),
                _logger);
    }

    private void assertMetricCountsCorrect(final long inRecords, final long outRecords, final long parsingExceptions,
                                           final long kafkaExceptions, final long consumerExceptions) {
        _periodicMetrics.run(); // Flush the metrics
        Assert.assertEquals(inRecords,
                _periodicMetrics.getCounters(_source._recordsInCountMetricName)
                        .stream().mapToLong(Long::longValue).sum());
        Assert.assertEquals(outRecords,
                _periodicMetrics.getCounters(_source._recordsOutCountMetricName)
                        .stream().mapToLong(Long::longValue).sum());
        Assert.assertEquals(parsingExceptions,
                _periodicMetrics.getCounters(_source._parsingExceptionCountMetricName)
                        .stream().mapToLong(Long::longValue).sum());
        Assert.assertEquals(kafkaExceptions,
                _periodicMetrics.getCounters(_source._kafkaExceptionCountMetricName)
                        .stream().mapToLong(Long::longValue).sum());
        Assert.assertEquals(consumerExceptions,
                _periodicMetrics.getCounters(_source._consumerExceptionCountMetricName)
                        .stream().mapToLong(Long::longValue).sum());
    }

    private static List<String> createValues(final String prefix, final int num) {
        final ImmutableList.Builder<String> valuesBuilder = ImmutableList.builder();
        for (int i = 0; i < num; i++) {
            valuesBuilder.add(prefix + i);
        }
        return valuesBuilder.build();
    }

    private static MockConsumer<String, String> createMockConsumer(
            final ConsumerRecords<String, String> consumerRecords) {
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC, PARTITION)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), 0L));
        for (final ConsumerRecord<String, String> record : consumerRecords) {
            consumer.addRecord(record);
        }
        return consumer;
    }

    private static ConsumerRecords<String, String> expectedConsumerRecords() {
        long offset = 0L;
        final List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (final String value : EXPECTED) {
            records.add(new ConsumerRecord<>(TOPIC, PARTITION, offset++, "" + offset, value));
        }
        return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records));
    }

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
