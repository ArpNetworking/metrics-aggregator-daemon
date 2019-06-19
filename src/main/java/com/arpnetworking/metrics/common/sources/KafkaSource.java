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
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Produce instances of <code> TODO </code> from a Kafka topic.
 *
 * @param <K> The data type of the key field to deserialize from the <code>Source</code>.
 * @param <V> The data type of the value field to deserialize from the <code>Source</code>.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class KafkaSource<K, V> extends BaseSource {

    private final KafkaConsumer<K, V> _consumer;
    private final ExecutorService _consumerExecutor;
    private final Logger _logger;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);


    @Override
    public void start() {
        //TODO
        //Executor.execute( kafka consumer loop )
    }

    @Override
    public void stop() {
        //TODO
        //Executor stop kafka loop
        //Executor.shutdown
    }

    @LogValue
    public Object toLogValue() {
        //TODO
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
//                .put("parser", _parser)
//                .put("tailer", _tailer)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @SuppressWarnings("unused")
    private KafkaSource(final Builder<K, V> builder) {
        this(builder, LOGGER);
    }

    /* package private */ KafkaSource(final Builder<K, V> builder, final Logger logger) {
        //TODO
        super(builder);
        _logger = logger;
        _consumer = builder._consumer;
        _consumerExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "KafkaConsumer"));
    }


    /**
     * Implementation of builder pattern for <code>KafkaSource</code>.
     *
     * @param <K> the type of the key field deserialized from the consumer.
     * @param <V> the type of the value field deserialized from the consumer.
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static class Builder<K, V> extends BaseSource.Builder<Builder<K, V>, KafkaSource<K, V>> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(KafkaSource::new);
        }

        //TODO
//        /**
//         * Sets <code>Deserializer</code>. Cannot be null.
//         *
//         * @param value The <code>Deserializer</code>.
//         * @return This instance of <code>Builder</code>.
//         */
//        public final Builder<K, V> setKeyDeserializer(final Deserializer<K> value) {
//            _keyDeserializer = value;
//            return this;
//        }
//
//        /**
//         * Sets <code>Deserializer</code>. Cannot be null.
//         *
//         * @param value The <code>Deserializer</code>.
//         * @return This instance of <code>Builder</code>.
//         */
//        public final Builder<K, V> setValueDeserializer(final Deserializer<V> value) {
//            _valueDeserializer = value;
//            return this;
//        }


        @Override
        protected Builder<K, V> self() {
            return this;
        }

        @NotNull
        private Deserializer<K> _keyDeserializer;
        @NotNull
        private Deserializer<V> _valueDeserializer;

        //    Properties props = new Properties();
        //    props.setProperty("bootstrap.servers", "localhost:9092");
        //    props.setProperty("group.id", "test");
        //    props.setProperty("enable.auto.commit", "true");
        //    props.setProperty("auto.commit.interval.ms", "1000");
        //    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        private Properties _props;

        private KafkaConsumer<K, V> _consumer;
    }
}
