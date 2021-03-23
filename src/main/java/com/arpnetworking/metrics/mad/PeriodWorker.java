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

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Actor that aggregates a particular slice of the data set over time and metric.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
/* package private */ final class PeriodWorker extends AbstractActorWithTimers {

    /**
     * Public constructor. Since this is an {@code Actor} this method should not be
     * called directly, but instead you should use {@code Props}.
     */
    PeriodWorker(
            final ActorRef aggregator,
            final Key key,
            final Duration period,
            final Duration idleTimeout,
            final Bucket.Builder bucketBuilder,
            final PeriodicMetrics periodicMetrics) {
        //_aggregator = aggregator;
        //_key = key;
        _period = period;
        //_idleTimeout = idleTimeout;
        _bucketBuilder = bucketBuilder;
        _periodicMetrics = periodicMetrics;
        //_hasReceivedRecords = false;
        _nextScheduledRotationTime = Optional.empty();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        _periodicMetrics.recordCounter("actors/period_worker/started", 1);

        /*
        timers().startPeriodicTimer(
                IDLE_CHECK_TIMER,
                IDLE_CHECK_MESSAGE,
                _idleTimeout);*/
    }

    @Override
    public void postStop() throws Exception {
        _periodicMetrics.recordCounter("actors/period_worker/stopped", 1);

        timers().cancel(IDLE_CHECK_TIMER);

        if (timers().isTimerActive(ROTATE_TIMER_KEY)) {
            timers().cancel(ROTATE_TIMER_KEY);
            _nextScheduledRotationTime = Optional.empty();

            // Force a rotation of ALL buckets using epoch zero
            performRotation(ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.systemDefault()));

            LOGGER.debug()
                    .setMessage("Shutdown forced rotations")
                    .log();
        }

        super.postStop();
    }

    @Override
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
        _periodicMetrics.recordCounter("actors/period_worker/restarted", 1);
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(Record.class, this::processRecord)
                .matchEquals(ROTATE_MESSAGE, m -> rotateAndSchedule())
                //.matchEquals(IDLE_CHECK_MESSAGE, m -> checkForIdle())
                .build();
    }

    private void rotateAndSchedule() {
        final ZonedDateTime now = ZonedDateTime.now();
        _nextScheduledRotationTime = Optional.empty();
        performRotation(now);
        scheduleRotation(now);
    }
/*
    private void checkForIdle() {
        // The check for both no scheduled rotations and having received records
        // is necessary to ensure that the idle message is only sent after at
        // least the minimum time has elapsed and no future rotations exist. If
        // this were only based on rotations then the idle trigger could fire
        // after the current rotation but before data for the next rotation
        // arrives, therefore not ensuring that the minimum time has elapsed.
        if (!_nextScheduledRotationTime.isPresent() && !_hasReceivedRecords) {
            _aggregator.tell(new Aggregator.PeriodWorkerIdle(_key), self());
        }
        _hasReceivedRecords = false;
    }
*/
    private void scheduleRotation(final ZonedDateTime now) {
        if (timers().isTimerActive(ROTATE_TIMER_KEY)) {
            timers().cancel(ROTATE_TIMER_KEY);
        }

        _nextScheduledRotationTime = getRotateAt();

        if (_nextScheduledRotationTime.isPresent()) {
            // If we we don't need to wait then just set the scheduled delay to 0.
            // If we need to wait a really small amount of time, set the delay to a minimum to avoid sleep thrashing.
            // Otherwise schedule the next rotation at the predicted time.
            // Finally, if we wake-up and there's nothing to rotate we'll just re-apply these rules.

            Duration timeToRotate = Duration.between(now, _nextScheduledRotationTime.get());
            if (timeToRotate.isNegative()) {
                timeToRotate = Duration.ZERO;
            } else if (timeToRotate.compareTo(MINIMUM_ROTATION_CHECK_INTERVAL) < 0) {
                timeToRotate = MINIMUM_ROTATION_CHECK_INTERVAL;
            }

            timers().startSingleTimer(ROTATE_TIMER_KEY, ROTATE_MESSAGE, timeToRotate);
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

    private void processRecord(final Record record) {
        // Mark the actor as having received records
        //_hasReceivedRecords = true;

        // Find an existing bucket for the record
        final Duration timeout = getPeriodTimeout(_period);
        final ZonedDateTime start = getStartTime(record.getTime(), _period);
        Bucket bucket = _bucketsByStart.get(start);

        // Create a new bucket if one does not exist
        if (bucket == null) {
            final Bucket newBucket = _bucketBuilder
                    .setStart(start)
                    .build();

            _bucketsByStart.put(
                    start,
                    newBucket);

            bucket = newBucket;

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

            // Ensure rotation is scheduled (now that we have data)
            if (!_nextScheduledRotationTime.isPresent() || expiration.isBefore(_nextScheduledRotationTime.get())) {
                scheduleRotation(ZonedDateTime.now());
            }
        } else if (!_nextScheduledRotationTime.isPresent()) {
            // This is a serious bug!
            LOGGER.error()
                    .setMessage("Rotation not already scheduled while adding to existing bucket")
                    .addData("bucket", bucket)
                    .addData("record", record)
                    .log();

            // But we can cover up the issue by scheduling a rotation
            scheduleRotation(ZonedDateTime.now());
        }

        // Add the record to the _existing_ bucket
        bucket.add(record);
    }

    /* package private */ void performRotation(final ZonedDateTime now) {
        final Map<ZonedDateTime, List<Bucket>> expiredBucketMap = _bucketsByExpiration.headMap(now);
        final List<Bucket> expiredBuckets = Lists.newArrayList();
        int closedBucketCount = 0;

        // Phase 1: Collect expired buckets
        for (final Map.Entry<ZonedDateTime, List<Bucket>> entry : expiredBucketMap.entrySet()) {
            expiredBuckets.addAll(entry.getValue());
        }
        expiredBucketMap.clear();

        // Phase 2: Close the expired buckets
        for (final Bucket bucket : expiredBuckets) {
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

    /* package private */ Optional<ZonedDateTime> getRotateAt() {
        return Optional.ofNullable(_bucketsByExpiration.firstEntry())
                .map(Map.Entry::getKey);
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

    //private boolean _hasReceivedRecords;
    private Optional<ZonedDateTime> _nextScheduledRotationTime;

    //private final ActorRef _aggregator;
    //private final Key _key;
    private final Duration _period;
    //private final Duration _idleTimeout;
    private final Bucket.Builder _bucketBuilder;
    private final NavigableMap<ZonedDateTime, Bucket> _bucketsByStart = new TreeMap<>();
    private final NavigableMap<ZonedDateTime, List<Bucket>> _bucketsByExpiration = new TreeMap<>();

    // WARNING: Consider carefully the volume of samples recorded.
    // PeriodicMetrics reduces the number of scopes creates, but each sample is
    // still stored in-memory until it is flushed.
    private final PeriodicMetrics _periodicMetrics;

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodWorker.class);
    private static final String IDLE_CHECK_TIMER = "PeriodicIdleCheckTimer";
    private static final Duration MINIMUM_ROTATION_CHECK_INTERVAL = Duration.ofMillis(100);
    private static final Duration MINIMUM_PERIOD_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration MAXIMUM_PERIOD_TIMEOUT = Duration.ofMinutes(10);
    private static final String ROTATE_TIMER_KEY = "ROTATION_TIMER_KEY";
    private static final String ROTATE_MESSAGE = "ROTATE_NOW";
    private static final String IDLE_CHECK_MESSAGE = "CHECK_FOR_IDLE_NOW";
}
