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
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

/**
 * A runnable wrapper for a <code>Consumer</code> that will continually poll
 * the consumer
 *
 * @param <T> the type of the values pulled from kafka records
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class RunnableConsumerImpl<T> implements RunnableConsumer {
    private ConsumerListener<T> _listener;
    private Consumer<?, T> _consumer;
    private volatile boolean _isRunning;

    /* package private */ RunnableConsumerImpl(final Builder<T> builder) {
        _consumer = builder._consumer;
        _listener = builder._listener;
        _isRunning = true;

        // TODO: initialize listeners, etc
    }

    @Override
    public void run() {
        //TODO: thread exception
        while (_isRunning) {

            ConsumerRecords<?, T> records = _consumer.poll(Duration.ofMillis(100) /* TODO(jjackson): Configurable duration */ );

            for (ConsumerRecord<?, T> record : records) {
                _listener.handle(record);
            }
        }
        //TODO: handle exceptions
    }

    @Override
    public void stop() {
        _isRunning = false;
    }

    /**
     * Implementation of builder pattern for <code>RunnableConsumerImpl</code>.
     *
     * @param <T> the type of the values pulled from kafka records
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static class Builder<T> extends OvalBuilder<RunnableConsumerImpl<T>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<RunnableConsumerImpl.Builder<T>, RunnableConsumerImpl<T>>) RunnableConsumerImpl::new);
        }

        /**
         * Sets the <code>ConsumerListener</code> instance. Cannot be null.
         *
         * @param listener The <code>ConsumerListener</code> instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<T> setListener(final ConsumerListener<T> listener) {
            _listener = listener;
            return this;
        }

        /**
         * Sets the <code>Consumer</code> instance. Cannot be null.
         *
         * @param consumer The <code>Consumer</code> instance.
         * @return This instance of {@link RunnableConsumerImpl.Builder}
         */
        public RunnableConsumerImpl.Builder<T> setConsumer(final Consumer<?, T> consumer) {
            _consumer = consumer;
            return this;
        }

        @NotNull
        private Consumer<?, T> _consumer;
        @NotNull
        private ConsumerListener<T> _listener;
    }
}
