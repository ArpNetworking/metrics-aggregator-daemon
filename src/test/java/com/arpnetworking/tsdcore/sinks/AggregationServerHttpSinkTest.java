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

import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.utility.BaseActorTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URI;
import java.util.Collection;


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
        _aggregationServerHttpSinkBuilder = new AggregationServerHttpSink.Builder()
                .setName("aggregation_server_http_sink_test")
                .setUri(URI.create("http://localhost:" + 0 + PATH))
                .setActorSystem(getSystem())
                .setMetricsFactory(_mockMetricsFactory);
    }

    @Test
    public void testSerialize() {
        final Collection<HttpPostSink.SerializedDatum> serializedData = _aggregationServerHttpSinkBuilder.build().serialize(
                new PeriodicData.Builder()
                        .setPeriod(java.time.Duration.ofMinutes(1))
                        .setStart(java.time.ZonedDateTime.now())
                        .setDimensions(new DefaultKey(ImmutableMap.of(
                                "service", "test_service",
                                "cluster", "test_cluster")))
                        .setData(ImmutableMultimap.of(
                                "metric",
                                new AggregatedData.Builder()
                                        .setSupportingData(new Object())
                                        .setStatistic(STATISTIC_FACTORY.getStatistic("count"))
                                        .setIsSpecified(true)
                                        .setValue(new DefaultQuantity.Builder()
                                                .setValue(3.02)
                                                .build())
                                        .setPopulationSize(3L)
                                        .build()))
                        .setMinRequestTime(java.time.ZonedDateTime.now())
                        .build()
        );
        Assert.assertEquals(((HttpPostSink.SerializedDatum) serializedData.toArray()[0]).getPopulationSize(), 3L);
    }


    private AggregationServerHttpSink.Builder _aggregationServerHttpSinkBuilder;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final String PATH = "/aggregation_server/post/path";

    @Mock
    private MetricsFactory _mockMetricsFactory;
}
