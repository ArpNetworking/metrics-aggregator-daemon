/*
 * Copyright 2020 Inscope Metrics
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Tests for the {@link DefaultMetric} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class DefaultMetricTest {

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");

    private final Supplier<DefaultMetric.Builder> _defaultMetricBuilder = () -> new DefaultMetric.Builder()
            .setType(MetricType.GAUGE)
            .setValues(ImmutableList.of(TestBeanFactory.createSample()))
            .setStatistics(ImmutableMap.of(
                    TP99_STATISTIC,
                    ImmutableList.of()));

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _defaultMetricBuilder.get(),
                Metric.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_defaultMetricBuilder.get());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualityTestHelper.testEquality(
                _defaultMetricBuilder.get(),
                new DefaultMetric.Builder()
                        .setType(MetricType.TIMER)
                        .setValues(ImmutableList.of(TestBeanFactory.createSample()))
                        .setStatistics(ImmutableMap.of(
                                MEDIAN_STATISTIC,
                                ImmutableList.of())),
                Metric.class);
    }

    @Test
    public void testToString() {
        final String asString = _defaultMetricBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
