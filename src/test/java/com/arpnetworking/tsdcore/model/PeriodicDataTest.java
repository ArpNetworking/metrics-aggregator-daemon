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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.EqualityTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.test.TestBeanFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.function.Supplier;

/**
 * Tests for the {@link PeriodicData} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class PeriodicDataTest {

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEDIAN_STATISTIC = STATISTIC_FACTORY.getStatistic("median");
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");

    private final Supplier<PeriodicData.Builder> _periodicDataBuilder = () -> new PeriodicData.Builder()
            .setPeriod(Duration.ofMinutes(1))
            .setStart(ZonedDateTime.now())
            .setDimensions(new DefaultKey(ImmutableMap.of("dKey", "dValue")))
            .setData(ImmutableMultimap.of(
                    "metric",
                    new AggregatedData.Builder()
                            .setSupportingData(new Object())
                            .setStatistic(TP99_STATISTIC)
                            .setIsSpecified(true)
                            .setValue(TestBeanFactory.createSample())
                            .setPopulationSize(1L)
                            .build()));

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _periodicDataBuilder.get(),
                PeriodicData.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_periodicDataBuilder.get());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualityTestHelper.testEquality(
                _periodicDataBuilder.get(),
                new PeriodicData.Builder()
                        .setPeriod(Duration.ofMinutes(2))
                        .setStart(ZonedDateTime.now().plusMinutes(1))
                        .setDimensions(new DefaultKey(ImmutableMap.of("dKey2", "dValue2")))
                        .setData(ImmutableMultimap.of(
                                "metric2",
                                new AggregatedData.Builder()
                                        .setSupportingData(new Object())
                                        .setStatistic(MEDIAN_STATISTIC)
                                        .setIsSpecified(false)
                                        .setValue(TestBeanFactory.createSample())
                                        .setPopulationSize(2L)
                                        .build())),
                PeriodicData.class);
    }

    @Test
    public void testToString() {
        final String asString = _periodicDataBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
