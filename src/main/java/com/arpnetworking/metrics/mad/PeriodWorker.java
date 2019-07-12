/*
 * Copyright 2015 Groupon.com
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
package com.arpnetworking.metrics.mad;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Responsible for managing aggregation buckets for a period.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
/* package private */ final class PeriodWorker implements Runnable {

    /**
     * Shutdown this <code>PeriodWorker</code>. Cannot be restarted.
     */
    public void shutdown() {
        _isRunning = false;
    }

    /**
     * Process a <code>Record</code>.
     *
     * @param record Instance of <code>Record</code> to process.
     */
    public void record(final Record record) {
        _recordQueue.add(record);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(
                (thread, throwable) -> LOGGER.error()
                        .setMessage("Unhandled exception")
                        .addData("periodWorker", PeriodWorker.this)
                        .setThrowable(throwable)
                        .log());

        while (_isRunning) {
            try {
                ZonedDateTime now = ZonedDateTime.now();
                final ZonedDateTime rotateAt = getRotateAt(now);
                Duration timeToRotate = Duration.between(now, rotateAt);
                while (_isRunning && timeToRotate.compareTo(Duration.ZERO) > 0) {
                    // Process records or sleep
                    Record recordToProcess = _recordQueue.poll();
                    if (recordToProcess != null) {
                        while (recordToProcess != null) {
                            process(recordToProcess);
                            recordToProcess = _recordQueue.poll();
                        }
                    } else {
                        Thread.sleep(Math.min(timeToRotate.toMillis(), 100));
                    }
                    // Recompute time to close
                    now = ZonedDateTime.now();
                    timeToRotate = Duration.between(now, rotateAt);
                }
                // Drain the record queue before rotating
                final List<Record> recordsToProcess = Lists.newArrayList();
                _recordQueue.drainTo(recordsToProcess);
                for (final Record recordToProcess : recordsToProcess) {
                    process(recordToProcess);
                }
                // Rotate
                rotate(now);
            } catch (final InterruptedException e) {
                Thread.interrupted();
                LOGGER.warn()
                        .setMessage("Interrupted waiting to close buckets")
                        .setThrowable(e)
                        .log();
                // CHECKSTYLE.OFF: IllegalCatch - Top level catch to prevent thread death
            } catch (final Exception e) {
                // CHECKSTYLE.ON: IllegalCatch
                LOGGER.error()
                        .setMessage("Aggregator failure")
                        .addData("periodWorker", this)
                        .setThrowable(e)
                        .log();
            }
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
                .put("period", _period)
                .put("bucketBuilder", _bucketBuilder)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /* package private */ void process(final Record record) {
        // Find an existing bucket for the record
        final Duration timeout = getPeriodTimeout(_period);
        final ZonedDateTime start = getStartTime(record.getTime(), _period);
        Bucket bucket = _bucketsByStart.get(start);

        // Create a new bucket if one does not exist
        if (bucket == null) {
            // Pre-emptively add the record to the _new_ bucket. This avoids
            // the race condition after indexing by expiration between adding
            // the record and closing the bucket.
            final Bucket newBucket = _bucketBuilder
                    .setStart(start)
                    .build();
            newBucket.add(record);

            // Resolve bucket creation race condition; either:
            // 1) We won and can proceed to index the new bucket
            // 2) We lost and can proceed to add data to the existing bucket
            bucket = _bucketsByStart.putIfAbsent(start, newBucket);
            if (bucket == null) {
                final ZonedDateTime expiration = max(ZonedDateTime.now().plus(timeout), start.plus(_period).plus(timeout));

                LOGGER.debug()
                        .setMessage("Created new bucket")
                        .addData("bucket", newBucket)
                        .addData("expiration", expiration)
                        .addData("trigger", record.getId())
                        .log();

                // Index the bucket by its expiration date; the expiration date is always in the future
                _bucketsByExpiration.compute(expiration, (dateTime, buckets) -> {
                    if (buckets == null) {
                        buckets = Lists.newArrayList();
                    }
                    buckets.add(newBucket);
                    return buckets;
                });

                // New bucket created and indexed with record
                return;
            }
        }

        // Add the record to the _existing_ bucket
        bucket.add(record);
    }

    /* package private */ void rotate(final ZonedDateTime now) {
        final Map<ZonedDateTime, List<Bucket>> expiredBucketMap = _bucketsByExpiration.headMap(now);
        final List<Bucket> expiredBuckets = Lists.newArrayList();
        int closedBucketCount = 0;

        // Phase 1: Collect expired buckets
        for (final ZonedDateTime key : expiredBucketMap.keySet()) {
            for (final Bucket bucket : _bucketsByExpiration.remove(key)) {
                expiredBuckets.add(bucket);
            }
        }

        // Phase 2: Close the expired buckets
        for (final Bucket bucket : expiredBuckets) {
            // Close the bucket
            // NOTE: The race condition between process and close is resolved in Bucket
            bucket.close();
            _bucketsByStart.remove(bucket.getStart());
            ++closedBucketCount;

            LOGGER.debug()
                    .setMessage("Bucket closed")
                    .addData("periodWorker", this)
                    .addData("bucket", bucket)
                    .addData("now", now)
                    .log();
        }

        LOGGER.debug().setMessage("Rotated").addData("count", closedBucketCount).log();
    }

    /* package private */ ZonedDateTime getRotateAt(final ZonedDateTime now) {
        final Map.Entry<ZonedDateTime, List<Bucket>> firstEntry = _bucketsByExpiration.firstEntry();
        final ZonedDateTime periodFirstExpiration = firstEntry == null ? null : firstEntry.getKey();
        if (periodFirstExpiration != null && periodFirstExpiration.isAfter(now)) {
            return periodFirstExpiration;
        }
        return now.plus(_rotationCheck);
    }

    /* package private */ static Duration getPeriodTimeout(final Duration period) {
        // TODO(vkoskela): Support separate configurable timeouts per period. [MAI-499]
        final Duration timeoutDuration = period.dividedBy(2);
        if (MINIMUM_PERIOD_TIMEOUT.compareTo(timeoutDuration) > 0) {
            return MINIMUM_PERIOD_TIMEOUT;
        }
        if (MAXIMUM_PERIOD_TIMEOUT.compareTo(timeoutDuration) < 0) {
            return MAXIMUM_PERIOD_TIMEOUT;
        }
        return timeoutDuration;
    }

    /* package private */ static ZonedDateTime getStartTime(final ZonedDateTime dateTime, final Duration period) {
        // This effectively uses Jan 1, 1970 at 00:00:00 as the anchor point
        // for non-standard bucket sizes (e.g. 18 min) that do not divide
        // equally into an hour or day. Such use cases are rather uncommon.
        final long periodMillis = period.toMillis();
        final long dateTimeMillis = dateTime.toInstant().toEpochMilli();
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateTimeMillis - (dateTimeMillis % periodMillis)), ZoneOffset.UTC);
    }

    /* package private */ static ZonedDateTime max(final ZonedDateTime dateTime1, final ZonedDateTime dateTime2) {
        if (dateTime1.isAfter(dateTime2)) {
            return dateTime1;
        }
        return dateTime2;
    }

    private PeriodWorker(final Builder builder) {
        _period = builder._period;
        _bucketBuilder = builder._bucketBuilder;
    }

    private volatile boolean _isRunning = true;

    private final Duration _period;
    private final Bucket.Builder _bucketBuilder;
    private final Duration _rotationCheck = Duration.ofMillis(100);
    private final BlockingQueue<Record> _recordQueue = new LinkedBlockingDeque<>();
    private final ConcurrentSkipListMap<ZonedDateTime, Bucket> _bucketsByStart = new ConcurrentSkipListMap<>();
    private final NavigableMap<ZonedDateTime, List<Bucket>> _bucketsByExpiration =
            Maps.synchronizedNavigableMap(new ConcurrentSkipListMap<>());

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodWorker.class);
    private static final Duration MINIMUM_PERIOD_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration MAXIMUM_PERIOD_TIMEOUT = Duration.ofMinutes(10);

    /**
     * <code>Builder</code> implementation for <code>PeriodWorker</code>.
     */
    public static final class Builder extends OvalBuilder<PeriodWorker> {

        /**
         * Public constructor.
         */
        Builder() {
            super(PeriodWorker::new);
        }

        /**
         * Set the period. Cannot be null or empty.
         *
         * @param value The periods.
         * @return This <code>Builder</code> instance.
         */
        public Builder setPeriod(final Duration value) {
            _period = value;
            return this;
        }

        /**
         * Set the <code>Bucket</code> <code>Builder</code>. Cannot be null.
         *
         * @param value The bucket builder.
         * @return This <code>Builder</code> instance.
         */
        public Builder setBucketBuilder(final Bucket.Builder value) {
            _bucketBuilder = value;
            return this;
        }

        @NotNull
        private Duration _period;
        @NotNull
        private Bucket.Builder _bucketBuilder;
    }
}
