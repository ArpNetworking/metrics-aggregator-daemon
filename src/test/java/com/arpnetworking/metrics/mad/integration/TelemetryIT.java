/*
 * Copyright 2019 Dropbox
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
package com.arpnetworking.metrics.mad.integration;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.impl.AugmentedHistogram;
import com.arpnetworking.metrics.impl.TsdMetrics;
import com.arpnetworking.metrics.impl.TsdMetricsFactory;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.test.TelemetryClient;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Integration test for streaming telemetry.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
public final class TelemetryIT {

    public TelemetryIT(final String statisticName, final double expectedResult) {
        _statistic = STATISTIC_FACTORY.getStatistic(statisticName);
        _expectedResult = expectedResult;
        _telemetryClient = TelemetryClient.getInstance();
    }

    @Before
    public void setUp() {
        _metricName = UUID.randomUUID().toString();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> createParameters() {
        return Arrays.asList(
                new Object[]{"count", 55.0d},
                new Object[]{"sum", 423.5d},
                new Object[]{"mean", 7.7d},
                new Object[]{"max", 11.0d},
                new Object[]{"min", 1.1d});
                // TODO(ville): enable with mad-2.0
                //new Object[]{"median", 7.7d},
                //new Object[]{"p25", 5.5d},
                //new Object[]{"p75", 9.9d});
    }

    @Test
    public void testFromSamples() throws InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Double> future = new CompletableFuture<>();

        try {
            _telemetryClient.subscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic,
                    future::complete);

            try (Metrics metrics = METRICS_FACTORY.create()) {
                for (int i = 1; i <= 10; ++i) {
                    for (int j = 1; j <= i; ++j) {
                        metrics.setGauge(_metricName, i * 1.1d);
                    }
                }
            }

            Assert.assertEquals(_expectedResult, future.get(30, TimeUnit.SECONDS), 0.0001);
        } finally {
            _telemetryClient.unsubscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic);
        }
    }


    @Test
    public void testFromAggregatedData() throws InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Double> future = new CompletableFuture<>();

        try {
            _telemetryClient.subscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic,
                    future::complete);

            sleepToBeginningOfSecond();

            try (Metrics metrics = METRICS_FACTORY.create()) {
                final TsdMetrics tsdMetrics = (TsdMetrics) metrics;
                tsdMetrics.recordAggregatedData(
                        _metricName,
                        new AugmentedHistogram.Builder()
                                .setMinimum(1.1)
                                .setMaximum(5.5)
                                .setSum(60.5)
                                .setHistogram(ImmutableMap.of(
                                        1.1, 1L,
                                        2.2, 2L,
                                        3.3, 3L,
                                        4.4, 4L,
                                        5.5, 5L))
                                .setPrecision(7)
                                .build());
            }
            try (Metrics metrics = METRICS_FACTORY.create()) {
                final TsdMetrics tsdMetrics = (TsdMetrics) metrics;
                tsdMetrics.recordAggregatedData(
                        _metricName,
                        new AugmentedHistogram.Builder()
                                .setMinimum(6.6)
                                .setMaximum(11.0)
                                .setSum(363.0)
                                .setHistogram(ImmutableMap.of(
                                        6.6, 6L,
                                        7.7, 7L,
                                        8.8, 8L,
                                        9.9, 9L,
                                        11.0, 10L))
                                .setPrecision(7)
                                .build());
            }

            Assert.assertEquals(_expectedResult, future.get(30, TimeUnit.SECONDS), 0.0001);
        } finally {
            _telemetryClient.unsubscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic);
        }
    }

    @Test
    public void testFromMixedSamplesAndAggregatedData() throws InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Double> future = new CompletableFuture<>();

        try {
            _telemetryClient.subscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic,
                    future::complete);

            sleepToBeginningOfSecond();

            // First mixed samples-aggregates unit of work
            try (Metrics metrics = METRICS_FACTORY.create()) {
                for (int i = 1; i <= 2; ++i) {
                    for (int j = 1; j <= i; ++j) {
                        metrics.setGauge(_metricName, i * 1.1d);
                    }
                }

                final TsdMetrics tsdMetrics = (TsdMetrics) metrics;
                tsdMetrics.recordAggregatedData(
                        _metricName,
                        new AugmentedHistogram.Builder()
                                .setMinimum(3.3)
                                .setMaximum(5.5)
                                .setSum(55.0)
                                .setHistogram(ImmutableMap.of(
                                        3.3, 3L,
                                        4.4, 4L,
                                        5.5, 5L))
                                .setPrecision(7)
                                .build());
            }

            // Second mixed samples-aggregates unit of work
            try (Metrics metrics = METRICS_FACTORY.create()) {
                for (int i = 6; i <= 8; ++i) {
                    for (int j = 1; j <= i; ++j) {
                        metrics.setGauge(_metricName, i * 1.1d);
                    }
                }

                final TsdMetrics tsdMetrics = (TsdMetrics) metrics;
                tsdMetrics.recordAggregatedData(
                        _metricName,
                        new AugmentedHistogram.Builder()
                                .setMinimum(9.9)
                                .setMaximum(11.0)
                                .setSum(199.1)
                                .setHistogram(ImmutableMap.of(
                                        9.9, 9L,
                                        11.0, 10L))
                                .setPrecision(7)
                                .build());
            }

            Assert.assertEquals(_expectedResult, future.get(30, TimeUnit.SECONDS), 0.0001);
        } finally {
            _telemetryClient.unsubscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic);
        }
    }

    private static void sleepToBeginningOfSecond() {
        while (System.currentTimeMillis() % 1000 >= 100) {
            try {
                Thread.sleep(1);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String _metricName;

    private final Statistic _statistic;
    private final double _expectedResult;
    private final TelemetryClient _telemetryClient;

    private static final MetricsFactory METRICS_FACTORY = TsdMetricsFactory.newInstance("TelemetryIT", "test");
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
}
