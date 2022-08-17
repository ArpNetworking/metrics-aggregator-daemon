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
package com.arpnetworking.metrics.mad;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
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
 * Tests for the {@link Bucket} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class BucketTest {

    @Before
    public void setUp() {
        _mocks = MockitoAnnotations.openMocks(this);
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

    @After
    public void after() throws Exception {
        _mocks.close();
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

        MatcherAssert.assertThat(
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

    private void addRequestTimeData(final Optional<ZonedDateTime> requestTime) {
        _bucket.add(
                new DefaultRecord.Builder()
                        .setTime(START)
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
                                        .setValues(ImmutableList.of(ONE))
                                        .build()))
                        .setRequestTime(requestTime.orElse(null))
                        .build());
    }

    private void requestTimeCase(
            final Optional<ZonedDateTime> first,
            final Optional<ZonedDateTime> second,
            final Optional<ZonedDateTime> expected) {
        addRequestTimeData(first);
        addRequestTimeData(second);

        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        Assert.assertEquals(expected, dataCaptor.getValue().getMinRequestTime());
    }

    @Test
    public void testRequestTimeEmptyEmpty() {
        requestTimeCase(Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testRequestTimeEmptyLow() {
        requestTimeCase(Optional.empty(), LOWTIME, LOWTIME);
    }

    @Test
    public void testRequestTimeEmptyHigh() {
        requestTimeCase(Optional.empty(), HIGHTIME, HIGHTIME);
    }

    @Test
    public void testRequestTimeLowEmpty() {
        requestTimeCase(LOWTIME, Optional.empty(), LOWTIME);
    }

    @Test
    public void testRequestTimeLowLow() {
        requestTimeCase(LOWTIME, LOWTIME, LOWTIME);
    }

    @Test
    public void testRequestTimeLowHigh() {
        requestTimeCase(LOWTIME, HIGHTIME, LOWTIME);
    }

    @Test
    public void testRequestTimeHighEmpty() {
        requestTimeCase(HIGHTIME, Optional.empty(), HIGHTIME);
    }

    @Test
    public void testRequestTimeHighLow() {
        requestTimeCase(HIGHTIME, LOWTIME, LOWTIME);
    }

    @Test
    public void testRequestTimeHighHigh() {
        requestTimeCase(HIGHTIME, HIGHTIME, HIGHTIME);
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

        MatcherAssert.assertThat(
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

        MatcherAssert.assertThat(
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
                                .setValue(new DefaultQuantity.Builder()
                                        .setValue(3.0)
                                        .setUnit(Unit.SECOND)
                                        .build())
                                .build()));
    }

    @Test
    public void testCalculatedValues() {
        _bucket.add(
                new DefaultRecord.Builder()
                        .setTime(START)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                        .setId(UUID.randomUUID().toString())
                        .setMetrics(ImmutableMap.of(
                                "testCalculatedValues/MyMetric",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setStatistics(ImmutableMap.of(
                                                STATISTIC_FACTORY.getStatistic("min"), cvl(1.0, 2.0),
                                                STATISTIC_FACTORY.getStatistic("max"), cvl(99.0, 100.0),
                                                STATISTIC_FACTORY.getStatistic("count"), cvl(2.0, 3.0),
                                                STATISTIC_FACTORY.getStatistic("sum"), cvl(252.0)
                                        ))
                                        .build()))
                        .build());
        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        final ImmutableMultimap<String, AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(5, data.size());

        MatcherAssert.assertThat(
                data.get("testCalculatedValues/MyMetric"),
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setPopulationSize(5L)
                                .setStatistic(COUNT_STATISTIC)
                                .setValue(q(5))
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(false)
                                .setPopulationSize(5L)
                                .setStatistic(SUM_STATISTIC)
                                .setValue(q(252))
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setPopulationSize(5L)
                                .setStatistic(MEAN_STATISTIC)
                                .setValue(q(50.4))
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setPopulationSize(5L)
                                .setStatistic(MAX_STATISTIC)
                                .setValue(q(100))
                                .build(),
                        new AggregatedData.Builder()
                                .setIsSpecified(true)
                                .setPopulationSize(5L)
                                .setStatistic(MIN_STATISTIC)
                                .setValue(q(1))
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

    private static ImmutableList<CalculatedValue<?>> cvl(final double... valueArray) {
        final ImmutableList.Builder<CalculatedValue<?>> calculatedValues = ImmutableList.builder();
        for (final double value : valueArray) {
            calculatedValues.add(cv(value));
        }
        return calculatedValues.build();
    }

    private static CalculatedValue<?> cv(final double value) {
        return ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                CalculatedValue.Builder.class,
                b1 -> b1.setValue(q(value)));
    }

    private static Quantity q(final double value) {
        return ThreadLocalBuilder.build(
                DefaultQuantity.Builder.class,
                b2 -> b2.setValue(value));
    }

    private Bucket _bucket;

    private LoadingCache<String, Optional<ImmutableSet<Statistic>>> _specifiedStatsCache = CacheBuilder.newBuilder()
            .build(new TestSpecifiedCacheLoader());

    private LoadingCache<String, Optional<ImmutableSet<Statistic>>> _dependentStatsCache = CacheBuilder.newBuilder()
            .build(new TestDependentCacheLoader());

    @Mock
    private Sink _sink;
    private AutoCloseable _mocks;

    private static final ZonedDateTime START = ZonedDateTime.parse("2015-02-05T00:00:00Z");
    private static final Optional<ZonedDateTime> LOWTIME = Optional.of(START.plusMinutes(27));
    private static final Optional<ZonedDateTime> HIGHTIME = Optional.of(LOWTIME.get().plusMinutes(10));


    private static final Quantity ONE = new DefaultQuantity.Builder().setValue(1.0).build();
    private static final Quantity TWO = new DefaultQuantity.Builder().setValue(2.0).build();
    private static final Quantity THREE = new DefaultQuantity.Builder().setValue(3.0).build();
    private static final Quantity SIX = new DefaultQuantity.Builder().setValue(6.0).build();

    private static final Quantity ONE_SECOND = new DefaultQuantity.Builder().setValue(1.0).setUnit(Unit.SECOND).build();
    private static final Quantity TWO_SECONDS = new DefaultQuantity.Builder().setValue(2000.0).setUnit(Unit.MILLISECOND).build();
    private static final Quantity THREE_SECONDS = new DefaultQuantity.Builder().setValue(3.0).setUnit(Unit.SECOND).build();

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MIN_STATISTIC = STATISTIC_FACTORY.getStatistic("min");
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");
    private static final Statistic MAX_STATISTIC = STATISTIC_FACTORY.getStatistic("max");
    private static final Statistic SUM_STATISTIC = STATISTIC_FACTORY.getStatistic("sum");
    private static final Statistic COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");

    private static final class TestSpecifiedCacheLoader extends CacheLoader<String, Optional<ImmutableSet<Statistic>>> {
        @Override
        public Optional<ImmutableSet<Statistic>> load(@Nullable final String key) {
            if ("testCalculatedValues/MyMetric".equals(key)) {
                return Optional.of(
                        ImmutableSet.of(
                                STATISTIC_FACTORY.getStatistic("min"),
                                STATISTIC_FACTORY.getStatistic("max"),
                                STATISTIC_FACTORY.getStatistic("count"),
                                STATISTIC_FACTORY.getStatistic("mean")
                        ));
            }
            return Optional.empty();
        }
    }

    private static final class TestDependentCacheLoader extends CacheLoader<String, Optional<ImmutableSet<Statistic>>> {
        @Override
        public Optional<ImmutableSet<Statistic>> load(@Nullable final String key) {
            if ("testCalculatedValues/MyMetric".equals(key)) {
                return Optional.of(
                        ImmutableSet.of(
                                STATISTIC_FACTORY.getStatistic("sum")
                        ));
            }
            return Optional.empty();
        }
    }
}
