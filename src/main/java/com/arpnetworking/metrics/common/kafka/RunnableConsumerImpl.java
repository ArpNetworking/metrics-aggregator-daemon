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

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.CheckWith;
import net.sf.oval.constraint.CheckWithCheck;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

/**
 * A runnable wrapper for a {@code Consumer} that will continually poll
 * the consumer.
 *
 * @param <V> the type of the values in kafka {@code ConsumerRecords}
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class RunnableConsumerImpl<V> implements RunnableConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunnableConsumerImpl.class);

    private final ConsumerListener<V> _listener;
    private final Consumer<?, V> _consumer;
    private final Duration _pollTime;
    private volatile boolean _isRunning;

    /* package private */ RunnableConsumerImpl(final Builder<V> builder) {
        _consumer = builder._consumer;
        _listener = builder._listener;
        _pollTime = builder._pollTime;
        _isRunning = true;
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(
                (thread, throwable) -> LOGGER.error()
                        .setMessage("Unhandled exception")
                        .setThrowable(throwable)
                        .log());

        while (isRunning()) {
            try {
                final ConsumerRecords<?, V> records = _consumer.poll(_pollTime);

                for (final ConsumerRecord<?, V> record : records) {
                    _listener.handle(record);
                }
                _consumer.commitSync();
            // CHECKSTYLE.OFF: IllegalCatch - Allow clients to decide how to handle exceptions
            } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
                _listener.handle(e);
            }
        }
        _consumer.close();
    }

    @Override
    public void stop() {
        _isRunning = false;
    }

    /**
     * Determine whether the consumer thread is running.
     *
     * @return true if running false if not running.
     */
    protected boolean isRunning() {
        return _isRunning;
    }

    /**
     * Implementation of builder pattern for {@link RunnableConsumerImpl}.
     *
     * @param <V> the type of the values pulled from kafka records
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static final class Builder<V> extends OvalBuilder<RunnableConsumerImpl<V>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<RunnableConsumerImpl.Builder<V>,
                    RunnableConsumerImpl<V>>) RunnableConsumerImpl::new);
        }

        /**
         * Sets the {@link ConsumerListener} instance. Cannot be null.
         *
         * @param listener The {@link ConsumerListener} instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<V> setListener(final ConsumerListener<V> listener) {
            _listener = listener;
            return this;
        }

        /**
         * Sets the {@code Consumer} instance. Cannot be null.
         *
         * @param consumer The {@code Consumer} instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<V> setConsumer(final Consumer<?, V> consumer) {
            _consumer = consumer;
            return this;
        }

        /**
         * Sets the duration the consumer will poll kafka for each consume. Cannot be null.
         *
         * @param pollTime The {@code Duration} instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<V> setPollTime(final Duration pollTime) {
            _pollTime = pollTime;
            return this;
        }

        @NotNull
        private Consumer<?, V> _consumer;
        @NotNull
        private ConsumerListener<V> _listener;
        @NotNull
        @CheckWith(value = PositiveDuration.class, message = "Poll duration must be positive.")
        private Duration _pollTime;

        private static class PositiveDuration implements CheckWithCheck.SimpleCheck {
            @Override
            public boolean isSatisfied(final Object validatedObject, final Object value) {
                return (value instanceof Duration) && !((Duration) value).isNegative();
            }

            private static final long serialVersionUID = 1L;
        }
    }
}
