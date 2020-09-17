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
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotNull;
import org.apache.commons.codec.digest.Md5Crypt;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class ReverseRateLimitingSource extends BaseSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReverseRateLimitingSource.class);
    private final Source _source;

    public ReverseRateLimitingSource(Builder builder) {
        super(builder);
        this._source = builder._source;
        this._source.attach(new ReverseRateLimitingObserver(this));
    }

    @Override
    public void start() {
        _source.start();
    }

    @Override
    public void stop() {
        _source.stop();
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
    static final class ReverseRateLimitingObserver implements Observer {
        private final ReverseRateLimitingSource _source;
        private final ConcurrentSkipListMap<String, Integer> _recentOccurrences = new ConcurrentSkipListMap<>();
        private final HashMap<String, Integer> _globalOccurrences = new HashMap<>();
        private final Integer _threshold = 10;
        private final Clock _clock = Clock.systemUTC();

        /* package private */ ReverseRateLimitingObserver(final ReverseRateLimitingSource source) {
            _source = source;
        }

        @Override
        public void notify(Observable observable, Object event) {
            if (!(event instanceof Record)) {
                LOGGER.error()
                        .setMessage("Observed unsupported event")
                        .addData("event", event)
                        .log();
                return;
            }

            final Record record = (Record) event;
            final Key key = new DefaultKey(record.getDimensions());
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
                                    value -> Md5Crypt.md5Crypt((value + dimensionKeys).getBytes())
                            )
                    );

            final String timeBucket = getCurrentBucketStart();


            // Update _recentOccurrences with the latest hashes
            final List<String> metricNamesToRetain = metricToHash.entrySet()
                    .stream().filter(entry -> updateAndFilterHash(entry.getValue(), timeBucket))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            // If we are retaining zero metrics then we can just drop the record, otherwise we need to determine
            // if we're sending all or only part of the record.
            if (metricNamesToRetain.size() > 0) {
                // If all of the metrics passed the filter then just forward the event
                // otherwise we need to build a new record that only retains the metrics remaining
                // after filtering
                if (metricNamesToRetain.size() != record.getMetrics().size()) {
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

        private boolean updateAndFilterHash(final String hash, final String timeBucket) {
            final Integer recent = _recentOccurrences.compute(
                    timeBucket + "_" + hash,
                    (bucket, value) -> value == null ? 1 : value + 1);
            final Integer global = _globalOccurrences.getOrDefault(hash, 0);
            return (global + recent) > _threshold;
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
    public static class Builder extends BaseSource.Builder<ReverseRateLimitingSource.Builder, ReverseRateLimitingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(ReverseRateLimitingSource::new);
        }

        /**
         * Sets wrapped source. Cannot be null.
         *
         * @param value The wrapped source.
         * @return This instance of <code>Builder</code>.
         */
        public final ReverseRateLimitingSource.Builder setSource(final Source value) {
            _source = value;
            return this;
        }


        @Override
        protected ReverseRateLimitingSource.Builder self() {
            return this;
        }

        @NotNull
        private Source _source;
    }

}
