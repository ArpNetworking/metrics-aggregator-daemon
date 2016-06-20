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
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the <code>FQDSN</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class FQDSNTest {

    @Test
    public void testBuilder() {
        final String expectedCluster = "MyCluster";
        final String expectedService = "MyService";
        final String expectedMetric = "MyMetric";
        final Statistic expectedStatistic = TP99_STATISTIC;

        final FQDSN fqdsn = new FQDSN.Builder()
                .setCluster(expectedCluster)
                .setService(expectedService)
                .setMetric(expectedMetric)
                .setStatistic(expectedStatistic)
                .build();


        Assert.assertEquals(expectedCluster, fqdsn.getCluster());
        Assert.assertEquals(expectedService, fqdsn.getService());
        Assert.assertEquals(expectedMetric, fqdsn.getMetric());
        Assert.assertEquals(expectedStatistic, fqdsn.getStatistic());
    }

    @Test
    public void testEqualsAndHashCode() {
        BuildableEqualsAndHashCodeTester.assertEqualsAndHashCode(
                new FQDSN.Builder()
                        .setCluster("MyClusterA")
                        .setService("MyServiceA")
                        .setMetric("MyMetricA")
                        .setStatistic(TP99_STATISTIC),
                new FQDSN.Builder()
                        .setCluster("MyClusterB")
                        .setService("MyServiceB")
                        .setMetric("MyMetricB")
                        .setStatistic(MEDIAN_STATISTIC));
    }

    @Test
    public void testToString() {
        final String asString = new FQDSN.Builder()
                .setCluster("MyClusterA")
                .setService("MyServiceA")
                .setMetric("MyMetricA")
                .setStatistic(TP99_STATISTIC)
                .build()
                .toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");
}
