/**
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the <code>PeriodicStatisticsSink</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
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

    @Test
    public void testFlushOnClose() {
        final Sink statisticsSink = _statisticsSinkBuilder.build();
        Mockito.verify(_mockMetricsFactory).create();
        Mockito.verify(_mockMetrics).resetCounter(COUNTER_NAME);

        statisticsSink.close();
        Mockito.verifyNoMoreInteractions(_mockMetricsFactory);
        Mockito.verify(_mockMetrics).close();
    }

    @Test
    public void testPeriodicFlush() throws InterruptedException {
        final ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        final Sink statisticsSink = new PeriodicStatisticsSink(_statisticsSinkBuilder, executor);
        Mockito.verify(executor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.<TimeUnit>any());

        final Runnable periodicRunnable = runnableCaptor.getValue();

        Mockito.verify(_mockMetricsFactory).create();
        Mockito.verify(_mockMetrics).resetCounter(COUNTER_NAME);

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
        final Sink statisticsSink = _statisticsSinkBuilder.build();
        Mockito.verify(_mockMetricsFactory).create();
        Mockito.verify(_mockMetrics).resetCounter(COUNTER_NAME);

        statisticsSink.recordAggregateData(TestBeanFactory.createPeriodicData());
        statisticsSink.close();
        Mockito.verify(_mockMetrics).incrementCounter(COUNTER_NAME, 1);
        Mockito.verify(_mockMetrics).close();
    }

    private PeriodicStatisticsSink.Builder _statisticsSinkBuilder;
    private Metrics _mockMetrics;
    private MetricsFactory _mockMetricsFactory;

    private static final String COUNTER_NAME = "sinks/periodic_statistics/periodic_statistics_sink_test/aggregated_data";
}
