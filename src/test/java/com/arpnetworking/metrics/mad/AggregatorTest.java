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
import com.arpnetworking.tsdaggregator.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Tests for the <code>Aggregator</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class AggregatorTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
       _aggregator = new Aggregator.Builder()
                .setSink(_sink)
                .setCounterStatistics(Collections.singleton(MAX_STATISTIC))
                .setTimerStatistics(Collections.singleton(MAX_STATISTIC))
                .setGaugeStatistics(Collections.singleton(MAX_STATISTIC))
                .setPeriods(Collections.singleton(Period.seconds(1)))
                .build();
        _aggregator.launch();
    }

    @After
    public void tearDown() {
        _aggregator.shutdown();
    }

    @Test
    public void testCloseAfterElapsed() throws InterruptedException {
        final DateTime dataTimeInThePast = new DateTime(DateTimeZone.UTC)
                .minus(Duration.standardSeconds(10));

        // Send data to aggregator
        _aggregator.notify(
                null,
                new DefaultRecord.Builder()
                        .setMetrics(Collections.singletonMap(
                                "MyMetric",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setValues(Collections.singletonList(
                                                new Quantity.Builder().setValue(1d).build()))
                                        .build()))
                        .setTime(dataTimeInThePast)
                        .setId(UUID.randomUUID().toString())
                        .setCluster("MyCluster")
                        .setService("MyService")
                        .setHost("MyHost")
                        .build());

        // Wait for the period to close
        Thread.sleep(3000);

        // Verify the aggregation was emitted
        Mockito.verify(_sink).recordAggregateData(_periodicDataCaptor.capture());
        Mockito.verifyNoMoreInteractions(_sink);

        final PeriodicData periodicData = _periodicDataCaptor.getValue();

        final List<Condition> conditions = periodicData.getConditions();
        Assert.assertTrue(conditions.isEmpty());

        final List<AggregatedData> data = periodicData.getData();
        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setHost("MyHost")
                .setStart(dataTimeInThePast.withMillisOfSecond(0))
                .setIsSpecified(false)
                .setPeriod(Period.seconds(1))
                .setPopulationSize(1L)
                .setSamples(Collections.emptyList())
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertEquals(2, data.size());
        Assert.assertThat(
                data,
                Matchers.containsInAnyOrder(
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setMetric("MyMetric")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setMetric("MyMetric")
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setIsSpecified(true)
                                .build()));
    }

    @Test
    public void testMultipleClusters() throws InterruptedException {
        final DateTime start = DateTime.parse("2015-02-05T00:00:00Z");

        _aggregator.notify(
                null,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setCluster("MyClusterA")
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(ONE))
                                        .build()))
                        .build());

        _aggregator.notify(
                null,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setCluster("MyClusterB")
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(TWO))
                                        .build()))
                        .build());

        // Wait for the period to close
        Thread.sleep(3000);

        // Verify the aggregation was emitted
        Mockito.verify(_sink, Mockito.times(2)).recordAggregateData(_periodicDataCaptor.capture());
        Mockito.verifyNoMoreInteractions(_sink);

        final List<AggregatedData> unifiedData = getCapturedData();
        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setHost("MyHost")
                .setStart(start)
                .setIsSpecified(false)
                .setPeriod(Period.seconds(1))
                .setPopulationSize(1L)
                .setSamples(Collections.emptyList())
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertThat(
                unifiedData,
                Matchers.containsInAnyOrder(
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setCluster("MyClusterA")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setCluster("MyClusterB")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setCluster("MyClusterA")
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setValue(ONE)
                                .setIsSpecified(true)
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setCluster("MyClusterB")
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setValue(TWO)
                                .setIsSpecified(true)
                                .build()));
    }

    @Test
    public void testMultipleServices() throws InterruptedException {
        final DateTime start = DateTime.parse("2015-02-05T00:00:00Z");

        _aggregator.notify(
                null,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setService("MyServiceA")
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(ONE))
                                        .build()))
                        .build());

        _aggregator.notify(
                null,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setService("MyServiceB")
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(TWO))
                                        .build()))
                        .build());

        // Wait for the period to close
        Thread.sleep(3000);

        // Verify the aggregation was emitted
        Mockito.verify(_sink, Mockito.times(2)).recordAggregateData(_periodicDataCaptor.capture());
        Mockito.verifyNoMoreInteractions(_sink);

        final List<AggregatedData> unifiedData = getCapturedData();
        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setHost("MyHost")
                .setStart(start)
                .setIsSpecified(false)
                .setPeriod(Period.seconds(1))
                .setPopulationSize(1L)
                .setSamples(Collections.emptyList())
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertThat(
                unifiedData,
                Matchers.containsInAnyOrder(
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setService("MyServiceA")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setService("MyServiceB")
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setService("MyServiceA")
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setIsSpecified(true)
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setService("MyServiceB")
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setValue(TWO)
                                .setIsSpecified(true)
                                .build()));
    }

    @Test
    public void testMultipleHosts() throws InterruptedException {
        final DateTime start = DateTime.parse("2015-02-05T00:00:00Z");

        _aggregator.notify(
                null,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setHost("MyHostA")
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(ONE))
                                        .build()))
                        .build());

        _aggregator.notify(
                null,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setHost("MyHostB")
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(TWO))
                                        .build()))
                        .build());

        // Wait for the period to close
        Thread.sleep(3000);

        // Verify the aggregation was emitted
        Mockito.verify(_sink, Mockito.times(2)).recordAggregateData(_periodicDataCaptor.capture());
        Mockito.verifyNoMoreInteractions(_sink);

        final List<AggregatedData> unifiedData = getCapturedData();
        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setStart(start)
                .setIsSpecified(false)
                .setPeriod(Period.seconds(1))
                .setPopulationSize(1L)
                .setSamples(Collections.emptyList())
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertThat(
                unifiedData,
                Matchers.containsInAnyOrder(
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .setHost("MyHostA")
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setStatistic(COUNT_STATISTIC)
                                        .build())
                                .setHost("MyHostB")
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setHost("MyHostA")
                                .setValue(ONE)
                                .setIsSpecified(true)
                                .build(),
                        builder
                                .setFQDSN(createFqdsnBuilder()
                                        .setStatistic(MAX_STATISTIC)
                                        .build())
                                .setHost("MyHostB")
                                .setValue(TWO)
                                .setIsSpecified(true)
                                .build()));
    }

    private List<AggregatedData> getCapturedData() {
        final List<PeriodicData> capturedPeriodicData = _periodicDataCaptor.getAllValues();
        Assert.assertEquals(2, capturedPeriodicData.size());
        Assert.assertEquals(2, capturedPeriodicData.get(0).getData().size());
        Assert.assertEquals(2, capturedPeriodicData.get(1).getData().size());
        final List<AggregatedData> unifiedData = Lists.newArrayList();
        unifiedData.addAll(capturedPeriodicData.get(0).getData());
        unifiedData.addAll(capturedPeriodicData.get(1).getData());
        return unifiedData;
    }

    private static FQDSN.Builder createFqdsnBuilder() {
        return new FQDSN.Builder()
                .setCluster("MyCluster")
                .setService("MyService")
                .setMetric("MyCounter");
    }

    private Aggregator _aggregator;
    @Captor
    private ArgumentCaptor<PeriodicData> _periodicDataCaptor;
    @Mock
    private Sink _sink;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MAX_STATISTIC = STATISTIC_FACTORY.getStatistic("max");
    private static final Statistic COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");

    private static final Quantity ONE = new Quantity.Builder().setValue(1.0).build();
    private static final Quantity TWO = new Quantity.Builder().setValue(2.0).build();
}
