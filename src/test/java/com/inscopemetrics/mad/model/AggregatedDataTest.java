/*
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
package com.inscopemetrics.mad.model;

import com.inscopemetrics.mad.statistics.Statistic;
import com.inscopemetrics.mad.statistics.StatisticFactory;
import com.inscopemetrics.mad.test.BuildableEqualsAndHashCodeTester;
import com.inscopemetrics.mad.test.TestBeanFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the AggregatedData class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class AggregatedDataTest {

    @Test
    public void testBuilder() {
        final Statistic expectedStatistic = TP99_STATISTIC;
        final Quantity expectedValue = TestBeanFactory.createSample();
        final boolean expectedIsSpecified = true;
        final long expectedPopulationSize = 111;

        final AggregatedData aggregatedData = new AggregatedData.Builder()
                .setStatistic(expectedStatistic)
                .setValue(expectedValue)
                .setIsSpecified(expectedIsSpecified)
                .setPopulationSize(expectedPopulationSize)
                .build();

        Assert.assertEquals(expectedStatistic, aggregatedData.getStatistic());
        Assert.assertEquals(expectedValue, aggregatedData.getValue());
        Assert.assertEquals(expectedValue.getValue(), aggregatedData.getValue().getValue(), 0.001);
        Assert.assertEquals(expectedIsSpecified, aggregatedData.isSpecified());
        Assert.assertEquals(expectedPopulationSize, aggregatedData.getPopulationSize());
    }

    @Test
    public void testEqualsAndHashCode() {
        BuildableEqualsAndHashCodeTester.assertEqualsAndHashCode(
                new AggregatedData.Builder()
                        .setStatistic(TP99_STATISTIC)
                        .setValue(TestBeanFactory.createSample())
                        .setIsSpecified(true)
                        .setPopulationSize(1L)
                        .setSupportingData(new Object()),
                new AggregatedData.Builder()
                        .setStatistic(MEDIAN_STATISTIC)
                        .setValue(TestBeanFactory.createSample())
                        .setIsSpecified(false)
                        .setPopulationSize(2L)
                        .setSupportingData(new Object()));
    }

    @Test
    public void testToString() {
        final String asString = new AggregatedData.Builder()
                .setStatistic(TP99_STATISTIC)
                .setValue(TestBeanFactory.createSample())
                .setIsSpecified(true)
                .setPopulationSize(1L)
                .build()
                .toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");
}
