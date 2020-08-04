/*
 * Copyright 2020 Dropbox
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
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.utility.BaseActorTest;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.net.MediaType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.net.URI;
import java.util.concurrent.TimeUnit;



/**
 * Tests for the {@link AggregationServerHttpSink}.
 *
 * @author Qinyan Li (lqy520s at hotmail dot com)
 */
public class AggregationServerHttpSinkTest extends BaseActorTest {

    @Before
    @Override
    public void startup() {
        super.startup();
        _wireMockServer = new WireMockServer(0);
        _wireMockServer.start();
        _wireMock = new WireMock(_wireMockServer.port());
        _aggregationServerHttpSinkBuilder = new AggregationServerHttpSink.Builder()
                .setName("aggregation_server_http_sink_test")
                .setUri(URI.create("http://localhost:" + _wireMockServer.port() + PATH))
                .setActorSystem(getSystem())
                .setMetricsFactory(_mockMetricsFactory);
        Mockito.doReturn(_mockMetrics).when(_mockMetricsFactory).create();
    }

    @After
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        _wireMockServer.stop();
    }

    @Test
    public void testPost() throws InterruptedException {
        // Fake a successful post to aggregation server
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse().withStatus(200)));

        final HistogramStatistic.Histogram histogramForTest = new HistogramStatistic.Histogram();
        histogramForTest.recordValue(1.0, 2);
        histogramForTest.recordValue(2.0, 3);
        _aggregationServerHttpSinkBuilder.build().recordAggregateData(
                new PeriodicData.Builder()
                        .setPeriod(java.time.Duration.ofMinutes(1))
                        .setStart(java.time.ZonedDateTime.now())
                        .setDimensions(new DefaultKey(ImmutableMap.of(
                                "service", "test_service",
                                "cluster", "test_cluster")))
                        .setData(ImmutableMultimap.of(
                                "metric1", new AggregatedData.Builder()
                                        .setSupportingData(new Object())
                                        .setStatistic(STATISTIC_FACTORY.getStatistic("count"))
                                        .setIsSpecified(true)
                                        .setValue(new DefaultQuantity.Builder()
                                                .setValue(3.02)
                                                .build())
                                        .setPopulationSize(3L)
                                        .build(),
                                "metric2", new AggregatedData.Builder()
                                        .setSupportingData(new HistogramStatistic.HistogramSupportingData.Builder()
                                                .setHistogramSnapshot(histogramForTest.getSnapshot())
                                                .setUnit(Unit.SECOND)
                                                .build())
                                        .setStatistic(STATISTIC_FACTORY.getStatistic("histogram"))
                                        .setIsSpecified(true)
                                        .setValue(new DefaultQuantity.Builder()
                                                .setValue(3.02)
                                                .build())
                                        .setPopulationSize(5L)
                                        .build()))
                        .setMinRequestTime(java.time.ZonedDateTime.now())
                        .build()
        );

        // Allow the request/response to complete
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo(MediaType.PROTOBUF.toString()));

        // Assert that data was sent
        _wireMock.verifyThat(2, requestPattern);

        // Verify that metrics has been recorded.
        Mockito.verify(_mockMetricsFactory, Mockito.times(2)).create();
        Mockito.verify(_mockMetrics, Mockito.times(2))
                .incrementCounter("sinks/http_post/aggregation_server_http_sink_test/success", 1);
        Mockito.verify(_mockMetrics, Mockito.times(2))
                .incrementCounter("sinks/http_post/aggregation_server_http_sink_test/status/2xx", 1);
        Mockito.verify(_mockMetrics, Mockito.times(2)).setTimer(
                Mockito.matches("sinks/http_post/aggregation_server_http_sink_test/queue_time"),
                Mockito.anyLong(),
                Mockito.any(TimeUnit.class));
        Mockito.verify(_mockMetrics, Mockito.times(2)).setTimer(
                Mockito.matches("sinks/http_post/aggregation_server_http_sink_test/request_latency"),
                Mockito.anyLong(),
                Mockito.any(TimeUnit.class));
        Mockito.verify(_mockMetrics).incrementCounter("sinks/http_post/aggregation_server_http_sink_test/samples_sent", 3);
        Mockito.verify(_mockMetrics).incrementCounter("sinks/http_post/aggregation_server_http_sink_test/samples_sent", 5);
        Mockito.verify(_mockMetrics, Mockito.times(2)).close();
    }


    private AggregationServerHttpSink.Builder _aggregationServerHttpSinkBuilder;
    private WireMockServer _wireMockServer;
    private WireMock _wireMock;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final String PATH = "/aggregation_server/post/path";

    @Mock
    private MetricsFactory _mockMetricsFactory;
    @Mock
    private Metrics _mockMetrics;
}
