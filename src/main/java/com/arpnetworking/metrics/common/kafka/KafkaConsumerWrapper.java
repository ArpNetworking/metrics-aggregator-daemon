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
 * @param <K> The type of the keys in the kafka <code>ConsumerRecord</code>
 * @param <V> The type of the values in the kafka <code>ConsumerRecord</code>
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class KafkaConsumerWrapper<K, V> implements Consumer<K, V> {

    private KafkaConsumer<K, V> _consumer;

    private KafkaConsumerWrapper(final Builder<K, V> builder) {
        _consumer = new KafkaConsumer<>(builder._configs);
        _consumer.subscribe(builder._topics);
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
    public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener listener) {
        _consumer.subscribe(topics, listener);
    }

    @Override
    public void subscribe(final Collection<String> topics) {
        _consumer.subscribe(topics);
    }

    @Override
    public void subscribe(final Pattern pattern, final ConsumerRebalanceListener listener) {
        _consumer.subscribe(pattern, listener);
    }

    @Override
    public void subscribe(final Pattern pattern) {
        _consumer.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        _consumer.unsubscribe();
    }

    @Override
    public void assign(final Collection<TopicPartition> partitions) {
        _consumer.assign(partitions);
    }

    /**
     * @deprecated Deprecated in Kafka 2.0
     */
    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return _consumer.poll(timeoutMs);
    }

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return _consumer.poll(timeout);
    }

    @Override
    public void commitSync() {
        _consumer.commitSync();
    }

    @Override
    public void commitSync(final Duration timeout) {
        _consumer.commitSync(timeout);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        _consumer.commitSync(offsets);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        _consumer.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        _consumer.commitAsync();
    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {
        _consumer.commitAsync(callback);
    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        _consumer.commitAsync(offsets, callback);
    }

    @Override
    public void seek(final TopicPartition partition, final long offset) {
        _consumer.seek(partition, offset);
    }

    @Override
    public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
        _consumer.seek(partition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(final Collection<TopicPartition> partitions) {
        _consumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(final Collection<TopicPartition> partitions) {
        _consumer.seekToEnd(partitions);
    }

    @Override
    public long position(final TopicPartition partition) {
        return _consumer.position(partition);
    }

    @Override
    public long position(final TopicPartition partition, final Duration timeout) {
        return _consumer.position(partition, timeout);
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition) {
        return _consumer.committed(partition);
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
        return _consumer.committed(partition, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return _consumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return _consumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
        return _consumer.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return _consumer.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
        return _consumer.listTopics(timeout);
    }

    @Override
    public void pause(final Collection<TopicPartition> partitions) {
        _consumer.pause(partitions);
    }

    @Override
    public void resume(final Collection<TopicPartition> partitions) {
        _consumer.resume(partitions);
    }

    @Override
    public Set<TopicPartition> paused() {
        return _consumer.paused();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
        return _consumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch,
                                                                   final Duration timeout) {
        return _consumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
        return _consumer.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions,
                                                      final Duration timeout) {
        return _consumer.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
        return _consumer.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
        return _consumer.endOffsets(partitions, timeout);
    }

    @Override
    public void close() {
        _consumer.close();
    }

    /**
     * @deprecated Deprecated in Kafka 2.0
     */
    @Override
    @Deprecated
    public void close(final long timeout, final TimeUnit timeUnit) {
        _consumer.close(timeout, timeUnit);
    }

    @Override
    public void close(final Duration timeout) {
        _consumer.close(timeout);
    }

    @Override
    public void wakeup() {
        _consumer.wakeup();
    }

    /**
     * Builder pattern class for <code>KafkaConsumerWrapper</code>.
     *
     * @param <K>The type of the keys in the kafka <code>ConsumerRecord</code>
     * @param <V> The type of the values in the kafka <code>ConsumerRecord</code>
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
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

        /**
         * Sets configuration options for the <code>Consumer</code>. Cannot be null.
         *
         * @param configs The configuration options
         * @return This instance of <code>Builder</code>.
         */
        public Builder<K, V> setConfigs(final Map<String, Object> configs) {
            _configs = configs;
            return this;
        }

        /**
         * Sets the topics that the <code>Consumer</code> will be subscribed to.
         *
         * @param topics the <code>Collection</code> of topics
         * @return This instance of <code>Builder</code>.
         */
        public Builder<K, V> setTopics(final Collection<String> topics) {
            _topics = topics;
            return this;
        }
    }
}
