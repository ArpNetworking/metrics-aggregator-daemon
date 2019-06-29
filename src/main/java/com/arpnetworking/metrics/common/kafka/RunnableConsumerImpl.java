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
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

/**
 * A runnable wrapper for a <code>Consumer</code> that will continually poll
 * the consumer.
 *
 * @param <V> the type of the values in kafka <code>ConsumerRecords</code>
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class RunnableConsumerImpl<V> implements RunnableConsumer {
    private ConsumerListener<V> _listener;
    private Consumer<?, V> _consumer;
    private Duration _pollTime;
    private volatile boolean _isRunning;
    private static final Logger LOGGER = LoggerFactory.getLogger(RunnableConsumerImpl.class);

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

                for (ConsumerRecord<?, V> record : records) {
                    _listener.handle(record);
                }
            // CHECKSTYLE.OFF: IllegalCatch - Allow clients to decide how to handle exceptions
            } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
                _listener.handle(e);
            }
        }
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
     * Implementation of builder pattern for <code>RunnableConsumerImpl</code>.
     *
     * @param <V> the type of the values pulled from kafka records
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static class Builder<V> extends OvalBuilder<RunnableConsumerImpl<V>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<RunnableConsumerImpl.Builder<V>,
                    RunnableConsumerImpl<V>>) RunnableConsumerImpl::new);
        }

        /**
         * Sets the <code>ConsumerListener</code> instance. Cannot be null.
         *
         * @param listener The <code>ConsumerListener</code> instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<V> setListener(final ConsumerListener<V> listener) {
            _listener = listener;
            return this;
        }

        /**
         * Sets the <code>Consumer</code> instance. Cannot be null.
         *
         * @param consumer The <code>Consumer</code> instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<V> setConsumer(final Consumer<?, V> consumer) {
            _consumer = consumer;
            return this;
        }

        /**
         * Sets the duration the consumer will poll kafka for each consume. Cannot be null.
         *
         * @param pollTime The <code>Duration</code> instance.
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
        private Duration _pollTime;
    }
}
