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
package com.arpnetworking.metrics.mad.model;

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.EqualityTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.test.TestBeanFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Tests for the {@link AggregatedData} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class AggregatedDataTest {

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");

    private final Supplier<AggregatedData.Builder> _aggregatedDataBuilder = () -> new AggregatedData.Builder()
            .setStatistic(TP99_STATISTIC)
            .setSupportingData(new Object())
            .setValue(TestBeanFactory.createSample())
            .setIsSpecified(true)
            .setPopulationSize(111L);

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _aggregatedDataBuilder.get(),
                AggregatedData.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_aggregatedDataBuilder.get());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualityTestHelper.testEquality(
                _aggregatedDataBuilder.get(),
                new AggregatedData.Builder()
                        .setStatistic(MEDIAN_STATISTIC)
                        .setValue(TestBeanFactory.createSample())
                        .setIsSpecified(false)
                        .setPopulationSize(2L)
                        .setSupportingData(new Object()),
                AggregatedData.class);
    }

    @Test
    public void testToString() {
        final String asString = _aggregatedDataBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
