/*
 * Copyright 2020 Dropbox.com
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
package com.arpnetworking.metrics.mad.sources;

import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.common.sources.BaseSource;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.sources.ratelimiter.AcceptListSink;
import com.arpnetworking.metrics.mad.sources.ratelimiter.AcceptListSource;
import com.arpnetworking.metrics.mad.sources.ratelimiter.AcceptListStore;
import com.arpnetworking.metrics.mad.sources.ratelimiter.AcceptListStoreUpdater;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.sf.oval.constraint.NotNull;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DistinctOriginLimitingSource extends BaseSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistinctOriginLimitingSource.class);
    private static final long FIVE_MINUTES_IN_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private final Source _source;
    private final DistinctOriginLimitingObserver _observer;
    private final ScheduledExecutorService _updateExecutor;
    private final AcceptListStoreUpdater _updater;

    public DistinctOriginLimitingSource(Builder builder) {
        super(builder);
        _source = builder._source;
        _observer = new DistinctOriginLimitingObserver(this, builder._hashFunction, builder._threshold);
        _source.attach(_observer);
        _updateExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "DistinctOriginLimiterSource"));
        _updater = new AcceptListStoreUpdater(builder._store, _observer, _observer);
    }

    @Override
    public void start() {
        _updater.run();
        _updateExecutor.scheduleWithFixedDelay(_updater, FIVE_MINUTES_IN_MILLIS, FIVE_MINUTES_IN_MILLIS, TimeUnit.MILLISECONDS);
        _source.start();
    }

    @Override
    public void stop() {
        _source.stop();
        _updateExecutor.shutdown();
    }

    // Overridden to make available for tests
    @Override
    protected void notify(Object event) {
        super.notify(event);
    }

    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("source", _source)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }


    /**
     * Package private for testing
     */
    static final class DistinctOriginLimitingObserver implements Observer, AcceptListSink, AcceptListSource {
        private final DistinctOriginLimitingSource _source;
        private final HashFunction _hashFunction;
        private final HashSet<String> _accepted = new HashSet<>();
        private final Integer _threshold;
        private final Clock _clock = Clock.systemUTC();
        private String _lastTimeBucket;
        private ConcurrentSkipListMap<String, Set<String>> _recentOccurrences = new ConcurrentSkipListMap<>();
        private ImmutableSet<String> _globalAcceptList = ImmutableSet.<String>builder().build();

        /* package private */ DistinctOriginLimitingObserver(
                final DistinctOriginLimitingSource source,
                final HashFunction hashFunction,
                final Integer threshold) {
            _source = source;
            _hashFunction = hashFunction;
            _threshold = threshold;
            _lastTimeBucket = getCurrentBucketStart();
        }

        @Override
        public void notify(@NonNull final Observable observable, @NonNull final Object event) {
            if (!(event instanceof Record)) {
                LOGGER.error()
                        .setMessage("Observed unsupported event")
                        .addData("event", event)
                        .log();
                return;
            }

            final Record record = (Record) event;
            final Key key = new DefaultKey(record.getDimensions());
            final String origin = record.getAnnotations().getOrDefault("origin", "unknown");
            LOGGER.trace()
                    .setMessage("Sending record to aggregation actor")
                    .addData("record", record)
                    .addData("key", key)
                    .log();

            final String dimensionKeys = record.getDimensions().keySet().stream()
                    .sorted()
                    .collect(Collectors.joining(","));

            final Map<String, String> metricToHash = record.getMetrics()
                    .keySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    value -> value,
                                    value -> _hashFunction.hashBytes((value + dimensionKeys).getBytes()).toString()
                            )
                    );

            final String timeBucket = getCurrentBucketStart();

            // Age out old data
            if (!_lastTimeBucket.equals(timeBucket)) {
               _recentOccurrences = new ConcurrentSkipListMap<>(_recentOccurrences.tailMap(timeBucket, true));
            }

            // Update _recentOccurrences with the latest hashes
            final List<String> metricNamesToRetain = metricToHash.entrySet()
                    .stream().filter(entry -> updateAndFilterHash(entry.getValue(), origin, timeBucket))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            // If we are retaining zero metrics then we can just drop the record, otherwise we need to determine
            // if we're sending all or only part of the record.
            if (metricNamesToRetain.size() > 0) {
                // If all of the metrics passed the filter then just forward the event
                // otherwise we need to build a new record that only retains the metrics remaining
                // after filtering
                if (metricNamesToRetain.size() == record.getMetrics().size()) {
                    _source.notify(event);
                } else {
                    // Pair down the metrics to only those we're letting through and then send them along as a new
                    // record
                    final ImmutableMap<String, Metric> metricsToRetain = record.getMetrics()
                            .entrySet()
                            .stream()
                            .filter(entry -> metricNamesToRetain.contains(entry.getKey()))
                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

                    _source.notify(new DefaultRecord.Builder()
                            .setId(record.getId())
                            .setTime(record.getTime())
                            .setAnnotations(record.getAnnotations())
                            .setDimensions(record.getDimensions())
                            .setMetrics(metricsToRetain)
                            .build());
                }
            }
        }

        @Override
        public void updateAcceptList(ImmutableSet<String> acceptList) {
            _globalAcceptList = acceptList;
            _accepted.removeAll(_globalAcceptList);
        }

        @Override
        public ImmutableSet<String> getAcceptList() {
            return ImmutableSet.copyOf(_accepted);
        }

        private boolean updateAndFilterHash(final String hash, final String origin, final String timeBucket) {
            // Return quickly for known good
            if ( _globalAcceptList.contains(hash)) {
                _accepted.add(hash);
                return true;
            } else if (_accepted.contains(hash)) {
                return true;
            }

            final int recent = _recentOccurrences.compute(
                    timeBucket + "_" + hash,
                    (bucket, value) -> {
                        if (value == null) {
                            value = new HashSet<>();
                        }

                        value.add(origin);
                        return value;
                    }).size();

            if (recent > _threshold) {
                _accepted.add(hash);
                // We don't bother removing from the _recentOccurrences map as it will be aged out
                // over time.
                return true;
            } else {
                return false;
            }
        }

        private String getCurrentBucketStart() {
            return "" + _clock.instant().truncatedTo(ChronoUnit.MINUTES).getEpochSecond();
        }
    }

    /**
     * Implementation of builder pattern for <code>ReverseRateLimitingSource</code>.
     *
     * @author Gil Markham (gmarkham at dropbox dot com)
     */
    public static class Builder extends BaseSource.Builder<DistinctOriginLimitingSource.Builder, DistinctOriginLimitingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DistinctOriginLimitingSource::new);
        }

        /**
         * Sets wrapped source. Cannot be null.
         *
         * @param value The wrapped source.
         * @return This instance of <code>Builder</code>.
         */
        public final DistinctOriginLimitingSource.Builder setSource(final Source value) {
            _source = value;
            return this;
        }

        /**
         * Sets store for persisting accepted metrics. Cannot be null.
         *
         * @param value The distinct origin store.
         * @return This instance of <code>Builder</code>.
         */
        public final DistinctOriginLimitingSource.Builder setStore(final AcceptListStore value) {
            _store = value;
            return this;
        }

        /**
         * Sets the hashing function to use for hashing metric+tag values.
         *
         * @param value Hash function to use, defaults to SHA256
         * @return This instance of <code>Builder</code>.
         */
        public final DistinctOriginLimitingSource.Builder setHashFunction(final HashFunction value) {
            _hashFunction = value;
            return this;
        }

        /**
         * Sets the threshold of distinct origins necessary to accept a metric+tag combination.
         *
         * @param value threshold of distinct origins
         * @return This instance of <code>Builder</code>.
         */
        public final DistinctOriginLimitingSource.Builder setThreshold(final Integer value) {
            _threshold = value;
            return this;
        }


        @Override
        protected DistinctOriginLimitingSource.Builder self() {
            return this;
        }

        @NotNull
        private Source _source;
        @NotNull
        private AcceptListStore _store;
        @NotNull
        private HashFunction _hashFunction = Hashing.sha256();
        @NotNull
        private Integer _threshold = 10;
    }

}
