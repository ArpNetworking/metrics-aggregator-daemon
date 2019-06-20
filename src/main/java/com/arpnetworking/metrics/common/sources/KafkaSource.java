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
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Produce instances of <code>Record</code> from the values of entries
 * from a Kafka topic. The key from the entries gets discarded
 *
 * @param <T> the type of data created by the source
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class KafkaSource<T> extends BaseSource {

    private final Consumer<?, T> _consumer;
    private final RunnableConsumer _runnableConsumer;
    private final ExecutorService _consumerExecutor;
    private final Logger _logger;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);


    @Override
    public void start() {
        _consumerExecutor.execute(_runnableConsumer);
    }

    @Override
    public void stop() {
        _runnableConsumer.stop();

        _consumerExecutor.shutdown();
        try {
            _consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);
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
    private KafkaSource(final Builder<T> builder) {
        this(builder, LOGGER);
    }

    /* package private */ KafkaSource(final Builder<T> builder, final Logger logger) {
        super(builder);
        _logger = logger;
        _consumer = builder._consumer;
        _runnableConsumer = new RunnableConsumerImpl.Builder<T>()
                .setConsumer(builder._consumer)
                .setListener(new LogConsumerListener<>())
                .setPollTime(builder._pollTime)
                .build();
        _consumerExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "KafkaConsumer"));
    }

    private class LogConsumerListener<V> implements ConsumerListener<V> {

        @Override
        public void handle(final ConsumerRecord<?, V> record) {
            final V value = record.value();
            KafkaSource.this.notify(value);
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
                        .addData("action", "stopping")
                        .setThrowable(throwable)
                        .log();
                _runnableConsumer.stop();
            } else {
                _logger.error()
                        .setMessage("Consumer thread error")
                        .addData("source", KafkaSource.this)
                        .addData("action", "stopping")
                        .setThrowable(throwable)
                        .log();
                _runnableConsumer.stop();
            }
        }
    }

    /**
     * Builder pattern class for <code>KafkaSource</code>.
     *
     * @param <T> the type of data created by the source
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static class Builder<T> extends BaseSource.Builder<Builder<T>, KafkaSource<T>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(KafkaSource::new);
        }

        /**
         * Sets <code>Consumer</code>. Cannot be null.
         *
         * @param consumer The <code>Consumer</code>.
         * @return This instance of <code>Builder</code>.
         */
        public final Builder<T> setConsumer(final Consumer<?, T> consumer) {
            _consumer = consumer;
            return this;
        }

        /**
         * Sets the duration the consumer will poll kafka for each consume. Cannot be null.
         *
         * @param millis The number of milliseconds to poll for.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public final Builder<T> setPollTimeMillis(final int millis) {
            _pollTime = Duration.ofMillis(millis);
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        @NotNull
        private Consumer<?, T> _consumer;
        @NotNull
        private Duration _pollTime;
    }
}
