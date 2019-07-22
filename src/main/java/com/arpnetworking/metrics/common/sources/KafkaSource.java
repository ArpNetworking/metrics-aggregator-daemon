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
import net.sf.oval.constraint.CheckWith;
import net.sf.oval.constraint.CheckWithCheck;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Produce instances of {@code Record} from the values of entries
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
    private final Parser<T, V> _parser;
    private final Logger _logger;
    private final Duration _shutdownAwaitTime;
    private final Duration _backoffTime;
    private final PeriodicMetrics _periodicMetrics;

    @Override
    public void start() {
        _consumerExecutor.execute(_runnableConsumer);
    }

    @Override
    public void stop() {
        _runnableConsumer.stop();

        _consumerExecutor.shutdown();
        try {
            _consumerExecutor.awaitTermination(_shutdownAwaitTime.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            LOGGER.warn()
                    .setMessage("Unable to shutdown kafka consumer executor")
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
        this(builder, LOGGER);
    }

    /* package private */ KafkaSource(final Builder<T, V> builder, final Logger logger) {
        super(builder);
        _logger = logger;
        _consumer = builder._consumer;
        _parser = builder._parser;
        _runnableConsumer = new RunnableConsumerImpl.Builder<V>()
                .setConsumer(builder._consumer)
                .setListener(new LogConsumerListener())
                .setPollTime(builder._pollTime)
                .build();
        _consumerExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "KafkaConsumer"));
        _shutdownAwaitTime = builder._shutdownAwaitTime;
        _backoffTime = builder._backoffTime;
        _periodicMetrics = builder._periodicMetrics;
    }

    private class LogConsumerListener implements ConsumerListener<V> {

        @Override
        public void handle(final ConsumerRecord<?, V> consumerRecord) {
            final T record;
            try {
                record = _parser.parse(consumerRecord.value());
            } catch (final ParsingException e) {
                _logger.error()
                        .setMessage("Failed to parse data")
                        .setThrowable(e)
                        .log();
                return;
            }
            KafkaSource.this.notify(record);
            _periodicMetrics.recordCounter("num_records", 1);
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
                _logger.error()
                        .setMessage("Consumer received Kafka Exception")
                        .addData("source", KafkaSource.this)
                        .addData("action", "sleeping")
                        .setThrowable(throwable)
                        .log();
                backoff(throwable);
            } else {
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
         * Sets the amount of time the {@link KafkaSource} will wait to shutdown the {@code RunnableConsumer} thread.
         * Default is 10 seconds. Cannot be null or negative.
         *
         * @param shutdownAwaitTime The {@code Duration} the {@link KafkaSource} will wait to shutdown
         *                          the {@code RunnableConsumer} thread.
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
         * Sets {@code PeriodicMetrics} for self instrumentation of {@link KafkaSource}.
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
