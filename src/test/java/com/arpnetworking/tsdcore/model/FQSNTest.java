/**
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.arpnetworking.utility.test.BuildableEqualsAndHashCodeTester;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the <code>FQSN</code> class.
 *
 * TODO(vkoskela): Enable dimensions. [MAI-449]
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class FQSNTest {

    @Test
    public void testBuilder() {
        final Statistic expectedStatistic = TP99_STATISTIC;
        final String expectedService = "MyService";
        final String expectedMetric = "MyMetric";
        final String expectedCluster = "MyCluster";
        final Period expectedPeriod = Period.minutes(5);
        final DateTime expectedStart = DateTime.now();
        final String expectedHost = "MyHost";

        final FQSN fqsn = new FQSN.Builder()
                .setCluster(expectedCluster)
                .setService(expectedService)
                .setMetric(expectedMetric)
                .setStatistic(expectedStatistic)
                .setPeriod(expectedPeriod)
                .setStart(expectedStart)
                //.addDimension("host", expectedHost)
                .build();

        Assert.assertEquals(expectedCluster, fqsn.getCluster());
        Assert.assertEquals(expectedService, fqsn.getService());
        Assert.assertEquals(expectedMetric, fqsn.getMetric());
        Assert.assertEquals(expectedStatistic, fqsn.getStatistic());
        Assert.assertEquals(expectedPeriod, fqsn.getPeriod());
        Assert.assertEquals(expectedStart, fqsn.getStart());
        //Assert.assertEquals(1, fqsn.getDimensions().size());
        //Assert.assertEquals(expectedHost, fqsn.getDimensions().get("host"));
    }

    @Test
    public void testBuilderSetDimensions() {
        final Statistic expectedStatistic = TP99_STATISTIC;
        final String expectedService = "MyService";
        final String expectedMetric = "MyMetric";
        final String expectedCluster = "MyCluster";
        final Period expectedPeriod = Period.minutes(5);
        final DateTime expectedStart = DateTime.now();
        final String expectedHost = "MyHost";

        final FQSN fqsn = new FQSN.Builder()
                .setCluster(expectedCluster)
                .setService(expectedService)
                .setMetric(expectedMetric)
                .setStatistic(expectedStatistic)
                .setPeriod(expectedPeriod)
                .setStart(expectedStart)
                //.setDimensions(ImmutableMap.of("host", expectedHost))
                .build();

        Assert.assertEquals(expectedCluster, fqsn.getCluster());
        Assert.assertEquals(expectedService, fqsn.getService());
        Assert.assertEquals(expectedMetric, fqsn.getMetric());
        Assert.assertEquals(expectedStatistic, fqsn.getStatistic());
        Assert.assertEquals(expectedPeriod, fqsn.getPeriod());
        Assert.assertEquals(expectedStart, fqsn.getStart());
        //Assert.assertEquals(1, fqsn.getDimensions().size());
        //Assert.assertEquals(expectedHost, fqsn.getDimensions().get("host"));
    }

    @Test
    public void testBuilderWithFQDSN() {
        final Statistic expectedStatistic = TP99_STATISTIC;
        final String expectedService = "MyService";
        final String expectedMetric = "MyMetric";
        final String expectedCluster = "MyCluster";
        final Period expectedPeriod = Period.minutes(5);
        final DateTime expectedStart = DateTime.now();
        final String expectedHost = "MyHost";

        final FQSN fqsn = new FQSN.Builder()
                .fromFQDSN(new FQDSN.Builder()
                        .setCluster(expectedCluster)
                        .setService(expectedService)
                        .setMetric(expectedMetric)
                        .setStatistic(expectedStatistic)
                        .build())
                .setPeriod(expectedPeriod)
                .setStart(expectedStart)
                //.addDimension("host", expectedHost)
                .build();

        Assert.assertEquals(expectedCluster, fqsn.getCluster());
        Assert.assertEquals(expectedService, fqsn.getService());
        Assert.assertEquals(expectedMetric, fqsn.getMetric());
        Assert.assertEquals(expectedStatistic, fqsn.getStatistic());
        Assert.assertEquals(expectedPeriod, fqsn.getPeriod());
        Assert.assertEquals(expectedStart, fqsn.getStart());
        //Assert.assertEquals(1, fqsn.getDimensions().size());
        //Assert.assertEquals(expectedHost, fqsn.getDimensions().get("host"));
    }

    @Test
    public void testEqualsAndHashCode() {
        final DateTime now = DateTime.now();
        BuildableEqualsAndHashCodeTester.assertEqualsAndHashCode(
                new FQSN.Builder()
                        .setStatistic(TP99_STATISTIC)
                        .setService("MyServiceA")
                        .setMetric("MyMetricA")
                        .setCluster("MyClusterA")
                        .setPeriod(Period.minutes(1))
                        .setStart(now),
                        //.addDimension("host", "MyHostA"),
                new FQSN.Builder()
                        .setStatistic(MEDIAN_STATISTIC)
                        .setService("MyServiceB")
                        .setMetric("MyMetricB")
                        .setCluster("MyClusterB")
                        .setPeriod(Period.minutes(5))
                        .setStart(now.plusDays(1)));
                        //.addDimension("host", "MyHostB"));
    }

    @Test
    public void testToString() {
        final String asString = new FQSN.Builder()
                .setStatistic(TP99_STATISTIC)
                .setService("MyService")
                .setMetric("MyMetric")
                .setCluster("MyCluster")
                .setPeriod(Period.minutes(1))
                .setStart(DateTime.now())
                //.addDimension("host", "MyHost")
                .build()
                .toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");
}
