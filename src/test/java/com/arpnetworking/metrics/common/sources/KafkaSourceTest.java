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

import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Unit tests for the <code>KafkaSource</code> class.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class KafkaSourceTest {
    private KafkaSource<String> _source;
    private static final List<String> EXPECTED = Arrays.asList("value0", "value1", "value2");
    private static final String TOPIC = "test_topic";
    private static final int PARTITION = 0;
    private static final int POLL_TIME_MILLIS = 1;

    @Before
    public void setUp() {
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC, PARTITION)));
        long offset = 0L;
        final Map<TopicPartition, Long> beginningOffsets = Maps.newHashMap();
        beginningOffsets.put(new TopicPartition(TOPIC, PARTITION), offset);
        consumer.updateBeginningOffsets(beginningOffsets);

        for (String value : EXPECTED) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, offset++, "" + offset, value));
        }

        _source = new KafkaSource.Builder<String>()
                .setName("MockConsumerSource")
                .setConsumer(consumer)
                .setPollTimeMillis(POLL_TIME_MILLIS)
                .build();
    }

    @Test(timeout = 1000)
    public void testSourceSuccess() {
        final List<String> actual = new ArrayList<>();
        final Lock lock = new ReentrantLock();
        final Condition received = lock.newCondition();
        final SynchronizedObserver observer = new SynchronizedObserver(lock, received, actual);

        try {
            lock.lock();
            _source.attach(observer);
            try {
                _source.start();

                while (actual.size() != EXPECTED.size()) {
                    received.await();
                }
            } catch (final InterruptedException ie) {
            } finally {
                _source.stop();
            }
            // CHECKSTYLE.OFF: IllegalCatch - Always release lock
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
        } finally {
            lock.unlock();
        }
        Assert.assertEquals(EXPECTED, actual);
    }

    private static class SynchronizedObserver implements Observer {
        private List<String> _values;
        private Condition _cond;
        private Lock _lock;

        SynchronizedObserver(final Lock lock, final Condition cond, final List<String> values) {
            _cond = cond;
            _lock = lock;
            _values = values;
        }

        @Override
        public void notify(final Observable observable, final Object o) {
            if (o instanceof String) {
                _lock.lock();
                _values.add((String) o);
                _cond.signal();
                _lock.unlock();
            }
        }
    }
}
