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

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.arpnetworking.utility.test.BuildableEqualsAndHashCodeTester;
import com.google.common.collect.Lists;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for the AggregatedData class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class AggregatedDataTest {

    @Test
    @SuppressWarnings("deprecation")
    public void testBuilder() {
        final Statistic expectedStatistic = TP99_STATISTIC;
        final String expectedService = "MyService";
        final String expectedHost = "MyHost";
        final String expectedMetric = "MyMetric";
        final String expectedCluster = "MyCluster";
        final Quantity expectedValue = TestBeanFactory.createSample();
        final boolean expectedIsSpecified = true;
        final DateTime expectedPeriodStart = new DateTime();
        final Period expectedPeriod = Period.minutes(5);
        final long expectedPopulationSize = 111;
        final List<Quantity> expectedSamples = Lists.newArrayList(TestBeanFactory.createSample(), TestBeanFactory.createSample());

        final AggregatedData aggregatedData = new AggregatedData.Builder()
                .setFQDSN(new FQDSN.Builder()
                        .setStatistic(expectedStatistic)
                        .setMetric(expectedMetric)
                        .setService(expectedService)
                        .setCluster(expectedCluster)
                        .build())
                .setHost(expectedHost)
                .setValue(expectedValue)
                .setStart(expectedPeriodStart)
                .setIsSpecified(expectedIsSpecified)
                .setPeriod(expectedPeriod)
                .setPopulationSize(expectedPopulationSize)
                .setSamples(expectedSamples)
                .build();

        Assert.assertEquals(expectedStatistic, aggregatedData.getFQDSN().getStatistic());
        Assert.assertEquals(expectedHost, aggregatedData.getHost());
        Assert.assertEquals(expectedMetric, aggregatedData.getFQDSN().getMetric());
        Assert.assertEquals(expectedValue, aggregatedData.getValue());
        Assert.assertEquals(expectedValue.getValue(), aggregatedData.getValue().getValue(), 0.001);
        Assert.assertEquals(expectedPeriodStart, aggregatedData.getPeriodStart());
        Assert.assertEquals(expectedPeriod, aggregatedData.getPeriod());
        Assert.assertEquals(expectedIsSpecified, aggregatedData.isSpecified());
        Assert.assertEquals(expectedPopulationSize, aggregatedData.getPopulationSize());
        Assert.assertEquals(expectedSamples, aggregatedData.getSamples());
        Assert.assertEquals(expectedCluster, aggregatedData.getFQDSN().getCluster());
        Assert.assertEquals(expectedService, aggregatedData.getFQDSN().getService());
    }

    @Test
    public void testEqualsAndHashCode() {
        BuildableEqualsAndHashCodeTester.assertEqualsAndHashCode(
                new AggregatedData.Builder()
                        .setFQDSN(new FQDSN.Builder()
                                .setStatistic(TP99_STATISTIC)
                                .setMetric("MyMetricA")
                                .setService("MyServiceA")
                                .setCluster("MyServiceA")
                                .build())
                        .setHost("MyHostA")
                        .setValue(TestBeanFactory.createSample())
                        .setStart(new DateTime())
                        .setIsSpecified(true)
                        .setPeriod(Period.minutes(1))
                        .setPopulationSize(1L)
                        .setSupportingData(new Object())
                        .setSamples(Lists.newArrayList(TestBeanFactory.createSample())),
                new AggregatedData.Builder()
                        .setFQDSN(new FQDSN.Builder()
                                .setStatistic(MEDIAN_STATISTIC)
                                .setMetric("MyMetricB")
                                .setService("MyServiceB")
                                .setCluster("MyServiceB")
                                .build())
                        .setHost("MyHostB")
                        .setValue(TestBeanFactory.createSample())
                        .setStart(new DateTime().plusDays(1))
                        .setIsSpecified(false)
                        .setPeriod(Period.minutes(5))
                        .setPopulationSize(2L)
                        .setSupportingData(new Object())
                        .setSamples(Lists.newArrayList(TestBeanFactory.createSample(), TestBeanFactory.createSample())));
    }

    @Test
    public void testToString() {
        final String asString = new AggregatedData.Builder()
                .setFQDSN(new FQDSN.Builder()
                        .setStatistic(TP99_STATISTIC)
                        .setMetric("MyMetricA")
                        .setService("MyServiceA")
                        .setCluster("MyServiceA")
                        .build())
                .setHost("MyHostA")
                .setValue(TestBeanFactory.createSample())
                .setStart(new DateTime())
                .setIsSpecified(true)
                .setPeriod(Period.minutes(1))
                .setPopulationSize(1L)
                .setSamples(Lists.newArrayList(TestBeanFactory.createSample()))
                .build()
                .toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");
}
