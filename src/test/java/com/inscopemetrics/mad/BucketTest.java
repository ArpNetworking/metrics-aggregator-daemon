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
package com.inscopemetrics.mad;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.inscopemetrics.mad.model.AggregatedData;
import com.inscopemetrics.mad.model.DefaultKey;
import com.inscopemetrics.mad.model.DefaultMetric;
import com.inscopemetrics.mad.model.DefaultRecord;
import com.inscopemetrics.mad.model.Key;
import com.inscopemetrics.mad.model.MetricType;
import com.inscopemetrics.mad.model.PeriodicData;
import com.inscopemetrics.mad.model.Quantity;
import com.inscopemetrics.mad.model.Unit;
import com.inscopemetrics.mad.sinks.Sink;
import com.inscopemetrics.mad.statistics.Statistic;
import com.inscopemetrics.mad.statistics.StatisticFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Tests for the <code>Bucket</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class BucketTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        _bucket = new Bucket.Builder()
                .setKey(new DefaultKey(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster")))
                .setSink(_sink)
                .setStart(START)
                .setPeriod(Duration.ofMinutes(1))
                .setSpecifiedCounterStatistics(ImmutableSet.of(MIN_STATISTIC))
                .setSpecifiedGaugeStatistics(ImmutableSet.of(MEAN_STATISTIC))
                .setSpecifiedTimerStatistics(ImmutableSet.of(MAX_STATISTIC))
                .setDependentCounterStatistics(ImmutableSet.of())
                .setDependentGaugeStatistics(ImmutableSet.of(COUNT_STATISTIC, SUM_STATISTIC))
                .setDependentTimerStatistics(ImmutableSet.of())
                .setSpecifiedStatistics(_specifiedStatsCache)
                .setDependentStatistics(_dependentStatsCache)
                .build();
    }

    @Test
    public void testCounter() {
        addData("MyCounter", MetricType.COUNTER, ONE, 10);
        addData("MyCounter", MetricType.COUNTER, TWO, 20);
        addData("MyCounter", MetricType.COUNTER, THREE, 30);
        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        final ImmutableMultimap<String, AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(2, data.size());

        Assert.assertThat(
                data.get("MyCounter"),
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setIsSpecified(false)
                                .setPopulationSize(3L)
                                .setStatistic(COUNT_STATISTIC)
                                .setValue(THREE)
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setPopulationSize(3L)
                                .setStatistic(MIN_STATISTIC)
                                .setValue(ONE)
                                .build()));
    }

    @Test
    public void testEmptyCounter() {
        _bucket.add(
                new DefaultRecord.Builder()
                        .setTime(START.plus(Duration.ofSeconds(10)))
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                        .setId(UUID.randomUUID().toString())
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(ImmutableList.of())
                                        .build()))
                        .build());
        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        final ImmutableMultimap<String, AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(0, data.size());
    }

    @Test
    public void testGauge() {
        addData("MyGauge", MetricType.GAUGE, TWO, 10);
        addData("MyGauge", MetricType.GAUGE, ONE, 20);
        addData("MyGauge", MetricType.GAUGE, THREE, 30);
        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        final ImmutableMultimap<String, AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(3, data.size());

        Assert.assertThat(
                data.get("MyGauge"),
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setStatistic(MEAN_STATISTIC)
                                .setPopulationSize(3L)
                                .setValue(TWO)
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(false)
                                .setPopulationSize(3L)
                                .setStatistic(SUM_STATISTIC)
                                .setValue(SIX)
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(false)
                                .setPopulationSize(3L)
                                .setStatistic(COUNT_STATISTIC)
                                .setValue(THREE)
                                .build()));
    }

    @Test
    public void testTimer() {
        addData("MyTimer", MetricType.TIMER, THREE_SECONDS, 10);
        addData("MyTimer", MetricType.TIMER, TWO_SECONDS, 20);
        addData("MyTimer", MetricType.TIMER, ONE_SECOND, 30);
        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        final ImmutableMultimap<String, AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(2, data.size());

        Assert.assertThat(
                data.get("MyTimer"),
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setIsSpecified(false)
                                .setPopulationSize(3L)
                                .setStatistic(COUNT_STATISTIC)
                                .setValue(THREE)
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setPopulationSize(3L)
                                .setStatistic(MAX_STATISTIC)
                                .setValue(new Quantity.Builder()
                                        .setValue(3.0)
                                        .setUnit(Unit.SECOND)
                                        .build())
                                .build()));
    }

    @Test
    public void testToString() {
        final String asString = new Bucket.Builder()
                .setKey(new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHost",
                        Key.SERVICE_DIMENSION_KEY, "MyService",
                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"
                )))
                .setSink(Mockito.mock(Sink.class))
                .setStart(ZonedDateTime.now())
                .setPeriod(Duration.ofMinutes(1))
                .setSpecifiedCounterStatistics(ImmutableSet.of(MIN_STATISTIC))
                .setSpecifiedGaugeStatistics(ImmutableSet.of(MEAN_STATISTIC))
                .setSpecifiedTimerStatistics(ImmutableSet.of(MAX_STATISTIC))
                .setDependentCounterStatistics(ImmutableSet.of())
                .setDependentGaugeStatistics(ImmutableSet.of(COUNT_STATISTIC, SUM_STATISTIC))
                .setDependentTimerStatistics(ImmutableSet.of())
                .setSpecifiedStatistics(_specifiedStatsCache)
                .setDependentStatistics(_dependentStatsCache)
                .build()
                .toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private void addData(final String name, final MetricType type, final Quantity value, final long offset) {
        _bucket.add(
                new DefaultRecord.Builder()
                        .setTime(START.plus(Duration.ofSeconds(offset)))
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                        .setId(UUID.randomUUID().toString())
                        .setMetrics(ImmutableMap.of(
                                name,
                                new DefaultMetric.Builder()
                                        .setType(type)
                                        .setValues(ImmutableList.of(value))
                                        .build()))
                        .build());
    }

    private Bucket _bucket;

    private LoadingCache<String, Optional<ImmutableSet<Statistic>>> _specifiedStatsCache = CacheBuilder.newBuilder()
            .build(new AbsentStatisticCacheLoader());

    private LoadingCache<String, Optional<ImmutableSet<Statistic>>> _dependentStatsCache = CacheBuilder.newBuilder()
            .build(new AbsentStatisticCacheLoader());

    @Mock
    private Sink _sink;

    private static final ZonedDateTime START = ZonedDateTime.parse("2015-02-05T00:00:00Z");

    private static final Quantity ONE = new Quantity.Builder().setValue(1.0).build();
    private static final Quantity TWO = new Quantity.Builder().setValue(2.0).build();
    private static final Quantity THREE = new Quantity.Builder().setValue(3.0).build();
    private static final Quantity SIX = new Quantity.Builder().setValue(6.0).build();

    private static final Quantity ONE_SECOND = new Quantity.Builder().setValue(1.0).setUnit(Unit.SECOND).build();
    private static final Quantity TWO_SECONDS = new Quantity.Builder().setValue(2000.0).setUnit(Unit.MILLISECOND).build();
    private static final Quantity THREE_SECONDS = new Quantity.Builder().setValue(3.0).setUnit(Unit.SECOND).build();

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MIN_STATISTIC = STATISTIC_FACTORY.getStatistic("min");
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");
    private static final Statistic MAX_STATISTIC = STATISTIC_FACTORY.getStatistic("max");
    private static final Statistic SUM_STATISTIC = STATISTIC_FACTORY.getStatistic("sum");
    private static final Statistic COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");

    private static final class AbsentStatisticCacheLoader extends CacheLoader<String, Optional<ImmutableSet<Statistic>>> {
        @Override
        public Optional<ImmutableSet<Statistic>> load(@Nullable final String key) {
            return Optional.empty();
        }
    }
}
