/**
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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the <code>PeriodWorker</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class PeriodWorkerTest {

    @Test
    public void testGetStartTime() {
        // 1-second period
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 0), Period.seconds(1)));
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 1), Period.seconds(1)));
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 500), Period.seconds(1)));
        Assert.assertEquals(
                createDateTime(0, 0, 7, 0),
                PeriodWorker.getStartTime(createDateTime(0, 0, 7, 999), Period.seconds(1)));

        // 1-minute period
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 0, 0), Period.minutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 0, 1), Period.minutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 1, 0), Period.minutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 30, 500), Period.minutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 59, 0), Period.minutes(1)));
        Assert.assertEquals(
                createDateTime(0, 11, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 11, 59, 500), Period.minutes(1)));

        // 15-minute period
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 45, 0, 0), Period.minutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 45, 0, 1), Period.minutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 45, 1, 0), Period.minutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 53, 57, 133), Period.minutes(15)));
        Assert.assertEquals(
                createDateTime(0, 45, 0, 0),
                PeriodWorker.getStartTime(createDateTime(0, 59, 59, 999), Period.minutes(15)));

        // 1-hour period
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 0, 0, 0), Period.hours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 0, 0, 1), Period.hours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 0, 1, 0), Period.hours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 1, 0, 0), Period.hours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 33, 57, 133), Period.hours(1)));
        Assert.assertEquals(
                createDateTime(13, 0, 0, 0),
                PeriodWorker.getStartTime(createDateTime(13, 59, 59, 999), Period.hours(1)));
    }

    @Test
    public void testGetPeriodTimeout() {
        Assert.assertEquals(new Duration(1000), PeriodWorker.getPeriodTimeout(Period.millis(500)));
        Assert.assertEquals(new Duration(7500), PeriodWorker.getPeriodTimeout(Period.seconds(15)));
        Assert.assertEquals(new Duration(30000), PeriodWorker.getPeriodTimeout(Period.seconds(60)));
        Assert.assertEquals(new Duration(30000), PeriodWorker.getPeriodTimeout(Period.minutes(1)));
        Assert.assertEquals(new Duration(300000), PeriodWorker.getPeriodTimeout(Period.minutes(10)));
        Assert.assertEquals(new Duration(600000), PeriodWorker.getPeriodTimeout(Period.minutes(20)));
        Assert.assertEquals(new Duration(600000), PeriodWorker.getPeriodTimeout(Period.minutes(30)));
        Assert.assertEquals(new Duration(600000), PeriodWorker.getPeriodTimeout(Period.hours(1)));
    }

    private static DateTime createDateTime(final int hour, final int minute, final int second, final int millisecond) {
        return new DateTime(2014, 1, 1, hour, minute, second, millisecond, DateTimeZone.UTC);
    }
}
