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
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Wrapper class for <code>KafkaSource</code>. Adds a builder pattern constructor.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class KafkaConsumerWrapper<K, V> implements Consumer<K, V> {

    private KafkaConsumer<K, V> _consumer;

    private KafkaConsumerWrapper(final Builder<K, V> builder) {
        _consumer = new KafkaConsumer<>(builder._configs);
        _consumer.subscribe(builder._topics);
    }

    public static class Builder<K, V> extends OvalBuilder<KafkaConsumerWrapper<K, V>> {

        @NotEmpty
        @NotNull
        private Map<String, Object> _configs;
        @NotEmpty
        @NotNull
        private Collection<String> _topics;

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder<K, V>, KafkaConsumerWrapper<K, V>>) KafkaConsumerWrapper::new);
        }

        public Builder setConfigs(Map<String, Object> configs) {
            _configs = configs;
            return this;
        }

        public Builder setTopics(Collection<String> topics) {
            _topics = topics;
            return this;
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        return _consumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return _consumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        _consumer.subscribe(topics, listener);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        _consumer.subscribe(topics);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        _consumer.subscribe(pattern, listener);
    }

    @Override
    public void subscribe(Pattern pattern) {
        _consumer.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        _consumer.unsubscribe();
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        _consumer.assign(partitions);
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(long timeoutMs) {
        return _consumer.poll(timeoutMs);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return _consumer.poll(timeout);
    }

    @Override
    public void commitSync() {
        _consumer.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        _consumer.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        _consumer.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        _consumer.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        _consumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        _consumer.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        _consumer.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        _consumer.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        _consumer.seek(partition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        _consumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        _consumer.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return _consumer.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return _consumer.position(partition, timeout);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return _consumer.committed(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return _consumer.committed(partition, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return _consumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return _consumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return _consumer.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return _consumer.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return _consumer.listTopics(timeout);
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        _consumer.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        _consumer.resume(partitions);
    }

    @Override
    public Set<TopicPartition> paused() {
        return _consumer.paused();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return _consumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return _consumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return _consumer.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return _consumer.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return _consumer.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return _consumer.endOffsets(partitions, timeout);
    }

    @Override
    public void close() {
        _consumer.close();
    }

    @Override
    @Deprecated
    public void close(long timeout, TimeUnit timeUnit) {
        _consumer.close(timeout, timeUnit);
    }

    @Override
    public void close(Duration timeout) {
        _consumer.close(timeout);
    }

    @Override
    public void wakeup() {
        _consumer.wakeup();
    }

}
