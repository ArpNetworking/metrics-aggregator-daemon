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

import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
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
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
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
                OBSERVABLE,
                new DefaultRecord.Builder()
                        .setMetrics(ImmutableMap.of(
                                "MyMetric",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setValues(Collections.singletonList(
                                                new Quantity.Builder().setValue(1d).build()))
                                        .build()))
                        .setTime(dataTimeInThePast)
                        .setId(UUID.randomUUID().toString())
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                        .build());

        // Wait for the period to close
        Thread.sleep(3000);

        // Verify the aggregation was emitted
        Mockito.verify(_sink).recordAggregateData(_periodicDataCaptor.capture());
        Mockito.verifyNoMoreInteractions(_sink);

        final PeriodicData periodicData = _periodicDataCaptor.getValue();

        final ImmutableMultimap<String, AggregatedData> data = periodicData.getData();
        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setIsSpecified(false)
                .setPopulationSize(1L)
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertEquals(2, data.size());
        Assert.assertThat(
                data.get("MyMetric"),
                Matchers.containsInAnyOrder(
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setIsSpecified(true)
                                .build()));
    }

    @Test
    public void testMultipleClusters() throws InterruptedException {
        final DateTime start = DateTime.parse("2015-02-05T00:00:00Z");

        _aggregator.notify(
                OBSERVABLE,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyClusterA"))
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(ONE))
                                        .build()))
                        .build());

        _aggregator.notify(
                OBSERVABLE,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyClusterB"))
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

        final List<AggregatedData> unifiedData = getCapturedData(
                "MyCounter",
                new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHost",
                        Key.SERVICE_DIMENSION_KEY, "MyService",
                        Key.CLUSTER_DIMENSION_KEY, "MyClusterA")),
                new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHost",
                        Key.SERVICE_DIMENSION_KEY, "MyService",
                        Key.CLUSTER_DIMENSION_KEY, "MyClusterB")));
        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setIsSpecified(false)
                .setPopulationSize(1L)
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertThat(
                unifiedData,
                Matchers.containsInAnyOrder(
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setValue(ONE)
                                .setIsSpecified(true)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setValue(TWO)
                                .setIsSpecified(true)
                                .build()));
    }

    @Test
    public void testMultipleServices() throws InterruptedException {
        final DateTime start = DateTime.parse("2015-02-05T00:00:00Z");

        _aggregator.notify(
                OBSERVABLE,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyServiceA",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(ONE))
                                        .build()))
                        .build());

        _aggregator.notify(
                OBSERVABLE,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHost",
                                        Key.SERVICE_DIMENSION_KEY, "MyServiceB",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
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

        final List<AggregatedData> unifiedData = getCapturedData(
                "MyCounter",
                new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHost",
                        Key.SERVICE_DIMENSION_KEY, "MyServiceA",
                        Key.CLUSTER_DIMENSION_KEY, "MyCluster")),
                new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHost",
                        Key.SERVICE_DIMENSION_KEY, "MyServiceB",
                        Key.CLUSTER_DIMENSION_KEY, "MyCluster")));

        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setIsSpecified(false)
                .setPopulationSize(1L)
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertThat(
                unifiedData,
                Matchers.containsInAnyOrder(
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setIsSpecified(true)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setValue(TWO)
                                .setIsSpecified(true)
                                .build()));
    }

    @Test
    public void testMultipleHosts() throws InterruptedException {
        final DateTime start = DateTime.parse("2015-02-05T00:00:00Z");

        _aggregator.notify(
                OBSERVABLE,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHostA",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                        .setMetrics(ImmutableMap.of(
                                "MyCounter",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(Collections.singletonList(ONE))
                                        .build()))
                        .build());

        _aggregator.notify(
                OBSERVABLE,
                TestBeanFactory.createRecordBuilder()
                        .setTime(start)
                        .setDimensions(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "MyHostB",
                                        Key.SERVICE_DIMENSION_KEY, "MyService",
                                        Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
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

        final List<AggregatedData> unifiedData = getCapturedData(
                "MyCounter",
                new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHostA",
                        Key.SERVICE_DIMENSION_KEY, "MyService",
                        Key.CLUSTER_DIMENSION_KEY, "MyCluster")),
                new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHostB",
                        Key.SERVICE_DIMENSION_KEY, "MyService",
                        Key.CLUSTER_DIMENSION_KEY, "MyCluster")));

        final AggregatedData.Builder builder = new AggregatedData.Builder()
                .setIsSpecified(false)
                .setPopulationSize(1L)
                .setValue(new Quantity.Builder().setValue(1d).build());
        Assert.assertThat(
                unifiedData,
                Matchers.containsInAnyOrder(
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(COUNT_STATISTIC)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setValue(ONE)
                                .setIsSpecified(true)
                                .build(),
                        builder
                                .setStatistic(MAX_STATISTIC)
                                .setValue(TWO)
                                .setIsSpecified(true)
                                .build()));
    }

    private List<AggregatedData> getCapturedData(
            final String metricName,
            final Key dimensionSetA,
            final Key dimensionSetB) {
        final List<PeriodicData> capturedPeriodicData = _periodicDataCaptor.getAllValues();
        Assert.assertEquals(2, capturedPeriodicData.size());
        if (capturedPeriodicData.get(0).getDimensions().equals(dimensionSetA)) {
            Assert.assertEquals(dimensionSetB, capturedPeriodicData.get(1).getDimensions());
        } else {
            Assert.assertEquals(dimensionSetB, capturedPeriodicData.get(0).getDimensions());
            Assert.assertEquals(dimensionSetA, capturedPeriodicData.get(1).getDimensions());
        }
        Assert.assertEquals(1, capturedPeriodicData.get(0).getData().keySet().size());
        Assert.assertEquals(metricName, capturedPeriodicData.get(0).getData().keySet().iterator().next());
        Assert.assertEquals(2, capturedPeriodicData.get(0).getData().size());
        Assert.assertEquals(2, capturedPeriodicData.get(1).getData().size());
        final List<AggregatedData> unifiedData = Lists.newArrayList();
        unifiedData.addAll(capturedPeriodicData.get(0).getData().get(metricName));
        unifiedData.addAll(capturedPeriodicData.get(1).getData().get(metricName));
        return unifiedData;
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
    private static final Observable OBSERVABLE = new Observable() {
        @Override
        public void attach(final Observer observer) { }

        @Override
        public void detach(final Observer observer) { }
    };
}
