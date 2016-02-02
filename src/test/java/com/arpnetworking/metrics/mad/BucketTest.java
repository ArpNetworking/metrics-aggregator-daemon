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

import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

/**
 * Tests for the <code>Bucket</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class BucketTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        _bucket = new Bucket.Builder()
                .setCluster("MyCluster")
                .setService("MyService")
                .setHost("MyHost")
                .setSink(_sink)
                .setStart(START)
                .setPeriod(Period.minutes(1))
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

        final Collection<AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(2, data.size());

        Assert.assertThat(
                data,
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyCounter")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(false)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setSamples(Collections.emptyList())
                                .setValue(THREE)
                                .build(),
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyCounter")
                                        .setStatistic(MIN_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(true)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setSamples(Collections.emptyList())
                                .setValue(ONE)
                                .build()));
    }

    @Test
    public void testGauge() {
        addData("MyGauge", MetricType.GAUGE, TWO, 10);
        addData("MyGauge", MetricType.GAUGE, ONE, 20);
        addData("MyGauge", MetricType.GAUGE, THREE, 30);
        _bucket.close();

        final ArgumentCaptor<PeriodicData> dataCaptor = ArgumentCaptor.forClass(PeriodicData.class);
        Mockito.verify(_sink).recordAggregateData(dataCaptor.capture());

        final Collection<AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(3, data.size());

        Assert.assertThat(
                data,
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyGauge")
                                        .setStatistic(MEAN_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(true)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setValue(TWO)
                                .build(),
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyGauge")
                                        .setStatistic(SUM_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(false)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setSamples(Collections.emptyList())
                                .setValue(SIX)
                                .build(),
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyGauge")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(false)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setSamples(Collections.emptyList())
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

        final Collection<AggregatedData> data = dataCaptor.getValue().getData();
        Assert.assertEquals(2, data.size());

        Assert.assertThat(
                data,
                Matchers.containsInAnyOrder(
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyTimer")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(false)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setSamples(Collections.emptyList())
                                .setValue(THREE)
                                .build(),
                        new AggregatedData.Builder()
                                .setFQDSN(new FQDSN.Builder()
                                        .setCluster("MyCluster")
                                        .setService("MyService")
                                        .setMetric("MyTimer")
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setHost("MyHost")
                                .setStart(START)
                                .setIsSpecified(true)
                                .setPeriod(Period.minutes(1))
                                .setPopulationSize(3L)
                                .setSamples(Collections.emptyList())
                                .setValue(new Quantity.Builder()
                                        .setValue(3.0)
                                        .setUnit(Unit.SECOND)
                                        .build())
                                .build()));
    }

    @Test
    public void testToString() {
        final String asString = new Bucket.Builder()
                .setCluster("MyCluster")
                .setService("MyService")
                .setHost("MyHost")
                .setSink(Mockito.mock(Sink.class))
                .setStart(new DateTime())
                .setPeriod(Period.minutes(1))
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
                        .setTime(START.plus(Duration.standardSeconds(offset)))
                        .setCluster("MyCluster")
                        .setService("MyService")
                        .setHost("MyHost")
                        .setId(UUID.randomUUID().toString())
                        .setMetrics(ImmutableMap.of(
                                name,
                                new DefaultMetric.Builder()
                                        .setType(type)
                                        .setValues(Collections.singletonList(value))
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

    private static final DateTime START = DateTime.parse("2015-02-05T00:00:00Z");

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
        public Optional<ImmutableSet<Statistic>> load(final String key) throws Exception {
            return Optional.absent();
        }
    }
}
