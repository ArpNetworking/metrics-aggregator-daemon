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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.Units;
import com.arpnetworking.metrics.common.kafka.ConsumerListener;
import com.arpnetworking.metrics.common.kafka.RunnableConsumer;
import com.arpnetworking.metrics.common.kafka.RunnableConsumerImpl;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.base.Stopwatch;
import net.sf.oval.constraint.CheckWith;
import net.sf.oval.constraint.CheckWithCheck;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Produce instances of {@link com.arpnetworking.metrics.mad.model.Record} from the values of entries
 * from a Kafka topic. The key from the entries gets discarded
 *
 * @param <T> the type of data created by the source
 * @param <V> the type of data of value in kafka {@code ConsumerRecords}
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class KafkaSource<T, V> extends BaseSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

    private final Consumer<?, V> _consumer;
    private final RunnableConsumer _runnableConsumer;
    private final ExecutorService _consumerExecutor;
    private final ExecutorService _parserExecutor;
    private final Parser<T, V> _parser;
    private final Logger _logger;
    private final Duration _shutdownAwaitTime;
    private final Duration _backoffTime;
    private final Integer _numWorkerThreads;
    private final BlockingQueue<V> _buffer;
    private final ParsingWorker _parsingWorker = new ParsingWorker();
    private final PeriodicMetrics _periodicMetrics;
    private final AtomicLong _currentRecordsProcessedCount = new AtomicLong(0);
    private final AtomicLong _currentRecordsIngestedCount = new AtomicLong(0);
    private final String _parsingTimeMetricName = "sources/kafka/" + getMetricSafeName() + "/parsing_time";
    // CHECKSTYLE.OFF: VisibilityModifierCheck - Package private for use in testing
    final String _recordsInCountMetricName = "sources/kafka/" + getMetricSafeName() + "/records_in";
    final String _recordsOutCountMetricName = "sources/kafka/" + getMetricSafeName() + "/records_out";
    final String _parsingExceptionCountMetricName = "sources/kafka/" + getMetricSafeName() + "/parsing_exceptions";
    final String _kafkaExceptionCountMetricName = "sources/kafka/" + getMetricSafeName() + "/kafka_exceptions";
    final String _consumerExceptionCountMetricName = "sources/kafka/" + getMetricSafeName() + "/consumer_exceptions";
    final String _queueSizeGaugeMetricName = "sources/kafka/" + getMetricSafeName() + "/queue_size";
    // CHECKSTYLE.ON: VisibilityModifierCheck

    @Override
    public void start() {
        _consumerExecutor.execute(_runnableConsumer);
        for (int i = 0; i < _numWorkerThreads; i++) {
            _parserExecutor.execute(_parsingWorker);
        }
    }

    @Override
    public void stop() {
        _runnableConsumer.stop();
        _consumerExecutor.shutdown();
        try {
            _consumerExecutor.awaitTermination(_shutdownAwaitTime.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            _logger.warn()
                    .setMessage("Unable to shutdown kafka consumer executor")
                    .setThrowable(e)
                    .log();
        } finally {
            _consumer.close();
        }

        _parsingWorker.stop();
        _parserExecutor.shutdown();
        try {
            _parserExecutor.awaitTermination(_shutdownAwaitTime.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            _logger.warn()
                    .setMessage("Unable to shutdown parsing worker executor")
                    .setThrowable(e)
                    .log();
        }
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("consumer", _consumer)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @SuppressWarnings("unused")
    private KafkaSource(final Builder<T, V> builder) {
        this(builder, LOGGER, new ArrayBlockingQueue<>(builder._bufferSize));
    }

    // NOTE: Package private for testing
    /* package private */ KafkaSource(final Builder<T, V> builder, final Logger logger) {
        this(builder, logger, new ArrayBlockingQueue<>(builder._bufferSize));
    }

    // NOTE: Package private for testing
    /* package private */ KafkaSource(final Builder<T, V> builder, final BlockingQueue<V> buffer) {
        this(builder, LOGGER, buffer);
    }

    private KafkaSource(final Builder<T, V> builder, final Logger logger, final BlockingQueue<V> buffer) {
        super(builder);
        _consumer = builder._consumer;
        _parser = builder._parser;
        _runnableConsumer = new RunnableConsumerImpl.Builder<V>()
                .setConsumer(builder._consumer)
                .setListener(new LogConsumerListener())
                .setPollTime(builder._pollTime)
                .build();
        _numWorkerThreads = builder._numWorkerThreads;
        _consumerExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "KafkaConsumer"));
        _parserExecutor = Executors.newFixedThreadPool(_numWorkerThreads);
        _shutdownAwaitTime = builder._shutdownAwaitTime;
        _backoffTime = builder._backoffTime;
        _periodicMetrics = builder._periodicMetrics;
        _periodicMetrics.registerPolledMetric(periodicMetrics ->
                periodicMetrics.recordCounter(_recordsOutCountMetricName,
                        _currentRecordsProcessedCount.getAndSet(0)));
        _periodicMetrics.registerPolledMetric(periodicMetrics ->
                periodicMetrics.recordCounter(_recordsInCountMetricName,
                        _currentRecordsIngestedCount.getAndSet(0)));
        _logger = logger;
        _buffer = buffer;
    }

    private class ParsingWorker implements Runnable {
        private volatile boolean _isRunning = true;

        @Override
        public void run() {
            while (_isRunning || !_buffer.isEmpty()) { // Empty the queue before stopping the workers
                final V value = _buffer.poll();
                _periodicMetrics.recordGauge(_queueSizeGaugeMetricName, _buffer.size());
                if (value != null) {
                    final T record;
                    try {
                        final Stopwatch parsingTimer = Stopwatch.createStarted();
                        record = _parser.parse(value);
                        parsingTimer.stop();
                        _periodicMetrics.recordTimer(_parsingTimeMetricName,
                                parsingTimer.elapsed(TimeUnit.NANOSECONDS), Optional.of(Units.NANOSECOND));
                    } catch (final ParsingException e) {
                        _periodicMetrics.recordCounter(_parsingExceptionCountMetricName, 1);
                        _logger.error()
                                .setMessage("Failed to parse data")
                                .setThrowable(e)
                                .log();
                        continue;
                    }
                    KafkaSource.this.notify(record);
                    _currentRecordsProcessedCount.getAndIncrement();
                } else {
                    // Queue is empty
                    try {
                        Thread.sleep(_backoffTime.toMillis());
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        stop();
                    }
                }
            }
        }

        public void stop() {
            _isRunning = false;
        }
    }

    private class LogConsumerListener implements ConsumerListener<V> {

        @Override
        public void handle(final ConsumerRecord<?, V> consumerRecord) {
            try {
                _buffer.put(consumerRecord.value());
                _currentRecordsIngestedCount.getAndIncrement();
                _periodicMetrics.recordGauge(_queueSizeGaugeMetricName, _buffer.size());
            } catch (final InterruptedException e) {
                _logger.info()
                        .setMessage("Consumer thread interrupted")
                        .addData("source", KafkaSource.this)
                        .addData("action", "stopping")
                        .setThrowable(e)
                        .log();
                _runnableConsumer.stop();
            }
        }

        @Override
        public void handle(final Throwable throwable) {
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                _logger.info()
                        .setMessage("Consumer thread interrupted")
                        .addData("source", KafkaSource.this)
                        .addData("action", "stopping")
                        .setThrowable(throwable)
                        .log();
                _runnableConsumer.stop();
            } else if (throwable instanceof KafkaException) {
                _periodicMetrics.recordCounter(_kafkaExceptionCountMetricName, 1);
                _logger.error()
                        .setMessage("Consumer received Kafka Exception")
                        .addData("source", KafkaSource.this)
                        .addData("action", "sleeping")
                        .setThrowable(throwable)
                        .log();
                backoff(throwable);
            } else {
                _periodicMetrics.recordCounter(_consumerExceptionCountMetricName, 1);
                _logger.error()
                        .setMessage("Consumer thread error")
                        .addData("source", KafkaSource.this)
                        .addData("action", "sleeping")
                        .setThrowable(throwable)
                        .log();
                backoff(throwable);
            }
        }

        private void backoff(final Throwable throwable) {
            try {
                Thread.sleep(_backoffTime.toMillis());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();

                _logger.info()
                        .setMessage("Sleep interrupted")
                        .addData("source", KafkaSource.this)
                        .addData("action", "stopping")
                        .setThrowable(throwable)
                        .log();

                _runnableConsumer.stop();
            }
        }
    }

    /**
     * Builder pattern class for {@link KafkaSource}.
     *
     * @param <T> the type of data created by the source
     * @param <V> the type of data of value in kafka {@code ConsumerRecords}
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static final class Builder<T, V> extends BaseSource.Builder<Builder<T, V>, KafkaSource<T, V>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(KafkaSource::new);
        }

        /**
         * Sets {@code Consumer}. Cannot be null.
         *
         * @param consumer The {@code Consumer}.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setConsumer(final Consumer<?, V> consumer) {
            _consumer = consumer;
            return this;
        }

        /**
         * Sets {@link Parser}. Cannot be null.
         *
         * @param value The {@link Parser}.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setParser(final Parser<T, V> value) {
            _parser = value;
            return this;
        }

        /**
         * Sets the duration the consumer will poll kafka for each consume. Cannot be null or negative.
         *
         * @param pollTime The {@code Duration} of each poll call made by the {@code KafkaConsumer}.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setPollTime(final Duration pollTime) {
            _pollTime = pollTime;
            return this;
        }

        /**
         * Sets the amount of time the {@link KafkaSource} will wait to shutdown the {@link RunnableConsumer} thread.
         * Default is 10 seconds. Cannot be null or negative.
         *
         * @param shutdownAwaitTime The {@code Duration} the {@link KafkaSource} will wait to shutdown
         *                          the {@link RunnableConsumer} thread.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setShutdownAwaitTime(final Duration shutdownAwaitTime) {
            _shutdownAwaitTime = shutdownAwaitTime;
            return this;
        }

        /**
         * Sets the amount of time the {@link KafkaSource} will wait to retry an operation after
         * receiving an exception. Default is 1 second. Cannot be null or negative.
         *
         * @param backoffTime The {@code Duration} the {@link KafkaSource} will wait to retry
         *                          an operation on exception.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setBackoffTime(final Duration backoffTime) {
            _backoffTime = backoffTime;
            return this;
        }

        /**
         * Sets the number of threads that will parse {@code ConsumerRecord}s from the {@code Consumer}.
         * Default is 1. Must be greater than or equal to 1.
         *
         * @param numWorkerThreads The number of parsing worker threads.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setNumWorkerThreads(final Integer numWorkerThreads) {
            _numWorkerThreads = numWorkerThreads;
            return this;
        }

        /**
         * Sets the size of the buffer to hold {@code ConsumerRecord}s from the {@code Consumer} before they are parsed.
         * Default is 1000. Must be greater than or equal to 1.
         *
         * @param bufferSize The size of the buffer.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setBufferSize(final Integer bufferSize) {
            _bufferSize = bufferSize;
            return this;
        }

        /**
         * Sets {@code PeriodicMetrics} for instrumentation of {@link KafkaSource}.
         *
         * @param periodicMetrics The {@code PeriodicMetrics} for the {@link KafkaSource}.
         * @return This instance of {@link KafkaSource.Builder}.
         */
        public Builder<T, V> setPeriodicMetrics(final PeriodicMetrics periodicMetrics) {
            _periodicMetrics = periodicMetrics;
            return this;
        }

        @Override
        protected Builder<T, V> self() {
            return this;
        }

        @NotNull
        private Consumer<?, V> _consumer;
        @NotNull
        private Parser<T, V> _parser;
        @NotNull
        @CheckWith(value = PositiveDuration.class, message = "Poll duration must be positive.")
        private Duration _pollTime;
        @NotNull
        @CheckWith(value = PositiveDuration.class, message = "Shutdown await time must be positive.")
        private Duration _shutdownAwaitTime = Duration.ofSeconds(10);
        @NotNull
        @CheckWith(value = PositiveDuration.class, message = "Backoff time must be positive.")
        private Duration _backoffTime = Duration.ofSeconds(1);
        @NotNull
        @Min(1)
        private Integer _numWorkerThreads = 1;
        @NotNull
        @Min(1)
        private Integer _bufferSize = 1000;
        @JacksonInject
        @NotNull
        private PeriodicMetrics _periodicMetrics;

        private static class PositiveDuration implements CheckWithCheck.SimpleCheck {
            @Override
            public boolean isSatisfied(final Object validatedObject, final Object value) {
                return (value instanceof Duration) && !((Duration) value).isNegative();
            }

            private static final long serialVersionUID = 1L;
        }
    }
}
