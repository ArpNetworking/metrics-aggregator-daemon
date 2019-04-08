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
package io.inscopemetrics.mad;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Tests for the <code>PeriodWorker</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class PeriodWorkerTest {

    @Test
    public void testGetStartTime() {
        // 1-second period
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 0), Duration.ofSeconds(1)));
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 1), Duration.ofSeconds(1)));
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 500), Duration.ofSeconds(1)));
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 999), Duration.ofSeconds(1)));

        // 1-minute period
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 0, 0), Duration.ofMinutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 0, 1), Duration.ofMinutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 1, 0), Duration.ofMinutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 30, 500), Duration.ofMinutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 59, 0), Duration.ofMinutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 59, 500), Duration.ofMinutes(1)));

        // 15-minute period
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 45, 0, 0), Duration.ofMinutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 45, 0, 1), Duration.ofMinutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 45, 1, 0), Duration.ofMinutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 53, 57, 133), Duration.ofMinutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 59, 59, 999), Duration.ofMinutes(15)));

        // 1-hour period
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 0, 0, 0), Duration.ofHours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 0, 0, 1), Duration.ofHours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 0, 1, 0), Duration.ofHours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 1, 0, 0), Duration.ofHours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 33, 57, 133), Duration.ofHours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 59, 59, 999), Duration.ofHours(1)));
    }

    @Test
    public void testGetPeriodTimeout() {
        Assert.assertEquals(Duration.ofMillis(1000), PeriodWorker.getPeriodTimeout(Duration.ofMillis(500)));
        Assert.assertEquals(Duration.ofMillis(7500), PeriodWorker.getPeriodTimeout(Duration.ofSeconds(15)));
        Assert.assertEquals(Duration.ofMillis(30000), PeriodWorker.getPeriodTimeout(Duration.ofSeconds(60)));
        Assert.assertEquals(Duration.ofMillis(30000), PeriodWorker.getPeriodTimeout(Duration.ofMinutes(1)));
        Assert.assertEquals(Duration.ofMillis(300000), PeriodWorker.getPeriodTimeout(Duration.ofMinutes(10)));
        Assert.assertEquals(Duration.ofMillis(600000), PeriodWorker.getPeriodTimeout(Duration.ofMinutes(20)));
        Assert.assertEquals(Duration.ofMillis(600000), PeriodWorker.getPeriodTimeout(Duration.ofMinutes(30)));
        Assert.assertEquals(Duration.ofMillis(600000), PeriodWorker.getPeriodTimeout(Duration.ofHours(1)));
    }

    private static ZonedDateTime createDateTime(
            final int hour,
            final int minute,
            final int second,
            final int millisecond) {
        return ZonedDateTime.of(2014, 1, 1, hour, minute, second, millisecond, ZoneOffset.UTC);
    }
}
