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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import net.sf.oval.exception.ConstraintsViolatedException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the <code>PeriodicStatisticsSink</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class PeriodicStatisticsSinkTest {

    @Before
    public void before() {
        _mockMetrics = Mockito.mock(Metrics.class);
        _mockMetricsFactory = Mockito.mock(MetricsFactory.class);
        Mockito.doReturn(_mockMetrics).when(_mockMetricsFactory).create();
        _statisticsSinkBuilder = new PeriodicStatisticsSink.Builder()
                .setName("periodic_statistics_sink_test")
                .setMetricsFactory(_mockMetricsFactory)
                .setIntervalInMilliseconds(60L);
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testValidationDuplicateMappedTarget() {
        new PeriodicStatisticsSink.Builder()
                .setMetricsFactory(_mockMetricsFactory)
                .setName("testValidationDuplicateMappedTarget")
                .setMappedDimensions(
                        ImmutableMap.of(
                                "foo", "abc",
                                "bar", "abc"))
                .build();
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testValidationDimensionCollision() {
        new PeriodicStatisticsSink.Builder()
                .setMetricsFactory(_mockMetricsFactory)
                .setName("testValidationDimensionCollision")
                .setDimensions(
                        ImmutableSet.of(
                                "abc"))
                .setMappedDimensions(
                        ImmutableMap.of(
                                "foo", "abc",
                                "bar", "def"))
                .build();
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testValidationDimensionCollisionWithPeriodDimensionName() {
        new PeriodicStatisticsSink.Builder()
                .setMetricsFactory(_mockMetricsFactory)
                .setName("testValidationDimensionCollisionWithPeriodDimensionName")
                .setDimensions(
                        ImmutableSet.of(
                                "_period"))
                .build();
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testValidationMappedDimensionCollisionWithPeriodDimensionName() {
        new PeriodicStatisticsSink.Builder()
                .setMetricsFactory(_mockMetricsFactory)
                .setName("testValidationMappedDimensionCollisionWithPeriodDimensionName")
                .setMappedDimensions(
                        ImmutableMap.of(
                                "foo", "abc",
                                "bar", "_period"))
                .build();
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testValidationDimensionCollisionAlthoughLogicallyEquivalent() {
        new PeriodicStatisticsSink.Builder()
                .setMetricsFactory(_mockMetricsFactory)
                .setName("testValidationDimensionCollisionAlthoughLogicallyEquivalent")
                .setDimensions(
                        ImmutableSet.of(
                                "abc"))
                .setMappedDimensions(
                        ImmutableMap.of(
                                "abc", "abc",
                                "bar", "def"))
                .build();
    }

    @Test
    public void testValidationDimension() {
        final PeriodicStatisticsSink sink = new PeriodicStatisticsSink.Builder()
                .setMetricsFactory(_mockMetricsFactory)
                .setName("testValidationDimension")
                .setDimensions(
                        ImmutableSet.of(
                                "ghi",
                                "foo"))
                .setMappedDimensions(
                        ImmutableMap.of(
                                "foo", "abc",
                                "bar", "def"))
                .build();
        assertEquals(
                ImmutableMultimap.<String, String>builder()
                        .put("ghi", "ghi") // from dimensions
                        .put("foo", "foo") // from dimensions
                        .put("foo", "abc") // from mapped dimensions
                        .put("bar", "def") // from mapped dimensions
                        .build(),
                sink.getMappedDimensions());
    }

    @Test
    public void testCreateKey() {
        final Key k = new DefaultKey(
                ImmutableMap.of(
                        "foo", "abc",
                        "bar", "def"));

        assertEquals(
                new DefaultKey(ImmutableMap.of(
                        "_period", "PT1M"
                )),
                PeriodicStatisticsSink.computeKey(
                        k,
                        "_period",
                        Duration.ofMinutes(1),
                        ImmutableMultimap.of()));

        assertEquals(
                new DefaultKey(ImmutableMap.of(
                        "_period", "PT1M",
                        "foo", "abc"
                )),
                PeriodicStatisticsSink.computeKey(
                        k,
                        "_period",
                        Duration.ofMinutes(1),
                        ImmutableMultimap.of(
                                "foo", "foo"
                        )));

        assertEquals(
                new DefaultKey(ImmutableMap.of(
                        "_period", "PT1M",
                        "bar", "abc"
                )),
                PeriodicStatisticsSink.computeKey(
                        k,
                        "_period",
                        Duration.ofMinutes(1),
                        ImmutableMultimap.of(
                                "foo", "bar"
                        )));

        assertEquals(
                new DefaultKey(ImmutableMap.of(
                        "_period", "PT1M",
                        "bar", "abc",
                        "foo", "def"
                )),
                PeriodicStatisticsSink.computeKey(
                        k,
                        "_period",
                        Duration.ofMinutes(1),
                        ImmutableMultimap.of(
                                "foo", "bar",
                                "bar", "foo"
                        )));
    }

    @Test
    public void testFlushOnClose() {
        final ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        final Sink statisticsSink = new PeriodicStatisticsSink(_statisticsSinkBuilder, executor);
        Mockito.verify(executor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any());

        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        statisticsSink.close();

        // The metrics are created when data is received and then replaced on
        // flush (but the latter metrics instance is never closed or written to)
        Mockito.verify(_mockMetricsFactory, Mockito.times(2)).create();
        Mockito.verify(_mockMetrics, Mockito.times(2)).resetCounter(COUNTER_NAME);
        Mockito.verify(_mockMetrics).incrementCounter(
                Mockito.matches(COUNTER_NAME),
                Mockito.anyLong());
        Mockito.verify(_mockMetrics).close();
    }

    @Test
    public void testPeriodicFlush() {
        final ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        final Sink statisticsSink = new PeriodicStatisticsSink(_statisticsSinkBuilder, executor);
        Mockito.verify(executor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any());

        final Runnable periodicRunnable = runnableCaptor.getValue();

        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        periodicRunnable.run();
        Mockito.verify(_mockMetrics, Mockito.times(1)).incrementCounter(COUNTER_NAME, 1);
        Mockito.verify(_mockMetrics, Mockito.times(1)).close();

        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        periodicRunnable.run();
        Mockito.verify(_mockMetrics, Mockito.times(2)).incrementCounter(COUNTER_NAME, 1);
        Mockito.verify(_mockMetrics, Mockito.times(2)).close();

        Mockito.verify(_mockMetricsFactory, Mockito.times(3)).create();
        Mockito.verify(_mockMetrics, Mockito.times(3)).resetCounter(COUNTER_NAME);

        statisticsSink.close();
        Mockito.verify(_mockMetrics, Mockito.times(3)).close();
    }

    @Test
    public void testRecordProcessedAggregateData() {
        final ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        final Sink statisticsSink = new PeriodicStatisticsSink(_statisticsSinkBuilder, executor);
        Mockito.verify(executor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any());

        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        statisticsSink.close();
        Mockito.verify(_mockMetrics).incrementCounter(COUNTER_NAME, 1);
        Mockito.verify(_mockMetrics, Mockito.atLeastOnce()).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRecordDimensions() {
        final ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        _statisticsSinkBuilder.setDimensions(
                ImmutableSet.of("host"));
        final Sink statisticsSink = new PeriodicStatisticsSink(_statisticsSinkBuilder, executor);
        Mockito.verify(executor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any());

        final Metrics metricsA = Mockito.mock(Metrics.class);
        final Metrics metricsB = Mockito.mock(Metrics.class);
        final Metrics metricsUnused = Mockito.mock(Metrics.class);
        Mockito.when(_mockMetricsFactory.create())
                .thenReturn(metricsA, metricsB, metricsUnused);

        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        statisticsSink.close();

        // The first key had a host annotation
        Mockito.verify(metricsA).incrementCounter(COUNTER_NAME, 1);
        Mockito.verify(metricsA).addAnnotations(
                MockitoHamcrest.argThat(
                    Matchers.allOf(
                            Matchers.hasKey("host"),
                            Matchers.hasKey("_period"),
                            Matchers.aMapWithSize(2))));
        Mockito.verify(metricsA).close();

        // The second key also had a host annotation
        Mockito.verify(metricsB).incrementCounter(COUNTER_NAME, 1);
        Mockito.verify(metricsB).addAnnotations(
                MockitoHamcrest.argThat(
                        Matchers.allOf(
                                Matchers.hasKey("host"),
                                Matchers.hasKey("_period"),
                                Matchers.aMapWithSize(2))));
        Mockito.verify(metricsB).close();

        // The metrics instances that replaced A, B and Global on flush (via close) were never used
        Mockito.verify(metricsUnused, Mockito.never()).close();
    }

    private PeriodicStatisticsSink.Builder _statisticsSinkBuilder;
    private Metrics _mockMetrics;
    private MetricsFactory _mockMetricsFactory;

    private static final String COUNTER_NAME = "sinks/periodic_statistics/periodic_statistics_sink_test/aggregated_data";
}
