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
import com.arpnetworking.metrics.impl.TsdMetricsFactory;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.test.TelemetryClient;
import org.junit.Assert;
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

    public TelemetryIT(final Statistic statistic, final double expectedResult) {
        _statistic = statistic;
        _expectedResult = expectedResult;
        _telemetryClient = TelemetryClient.getInstance();
        _metricName = UUID.randomUUID().toString();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> createParameters() {
        return Arrays.asList(
                new Object[]{STATISTIC_FACTORY.getStatistic("count"), 10.0d},
                new Object[]{STATISTIC_FACTORY.getStatistic("sum"), 423.5d},
                new Object[]{STATISTIC_FACTORY.getStatistic("mean"), 42.35d},
                new Object[]{STATISTIC_FACTORY.getStatistic("max"), 110d},
                new Object[]{STATISTIC_FACTORY.getStatistic("min"), 1.1d});
                // TODO(ville): enable with mad-2.0
                //new Object[]{STATISTIC_FACTORY.getStatistic("median"), 33.55d},
                //new Object[]{STATISTIC_FACTORY.getStatistic("p25"), 9.9d},
                //new Object[]{STATISTIC_FACTORY.getStatistic("p75"), 70.4d});
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
                    metrics.setGauge(_metricName, i * i * 1.1d);
                }
            }

            Assert.assertEquals(_expectedResult, future.get(5, TimeUnit.SECONDS), 0.0001);
        } finally {
            _telemetryClient.unsubscribe(
                    "TelemetryIT",
                    _metricName,
                    _statistic);
        }
    }

    private final Statistic _statistic;
    private final double _expectedResult;
    private final TelemetryClient _telemetryClient;
    private final String _metricName;

    private static final MetricsFactory METRICS_FACTORY = TsdMetricsFactory.newInstance("TelemetryIT", "test");
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
}
