/*
 * Copyright 2020 Dropbox Inc.
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Tests for the {@link PeriodicDataToProtoConverter} class.
 *
 * @author William Ehlhardt (whale at dropbox dot com)
 */
public class PeriodicDataToProtoConverterTest {
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");

    @Test
    public void conversionTest() {
        final PeriodicData data = new PeriodicData.Builder()
                .setDimensions(
                        new DefaultKey(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "host-test",
                                        Key.SERVICE_DIMENSION_KEY, "service-test",
                                        Key.CLUSTER_DIMENSION_KEY, "cluster-test")))
                .setData(ImmutableMultimap.of("metric-test", new AggregatedData.Builder()
                        .setStatistic(MEAN_STATISTIC)
                        .setValue(new DefaultQuantity.Builder().setValue((double) 9999).setUnit(Unit.BIT).build())
                        .setIsSpecified(true)
                        .setPopulationSize((long) 1337).build()))
                .setPeriod(Duration.ofMinutes(5))
                .setStart(ZonedDateTime.parse("2020-06-04T16:04:38.424-07:00[America/Los_Angeles]"))
                .setMinRequestTime(ZonedDateTime.parse("2019-01-03T05:06:07.890-07:00[America/Los_Angeles]"))
                .build();

        final List<Messages.StatisticSetRecord> converted = PeriodicDataToProtoConverter.convert(data);

        Assert.assertEquals(1, converted.size());
        Assert.assertEquals(
                "service: \"service-test\"\n"
                + "cluster: \"cluster-test\"\n"
                + "metric: \"metric-test\"\n"
                + "period: \"PT5M\"\n"
                + "period_start: \"2020-06-04T16:04:38.424-07:00[America/Los_Angeles]\"\n"
                + "statistics {\n"
                + "  statistic: \"mean\"\n"
                + "  value: 1249.875\n"
                + "  unit: \"BYTE\"\n"
                + "  user_specified: true\n"
                + "}\n"
                + "dimensions {\n"
                + "  key: \"host\"\n"
                + "  value: \"host-test\"\n"
                + "}\n"
                + "dimensions {\n"
                + "  key: \"service\"\n"
                + "  value: \"service-test\"\n"
                + "}\n"
                + "dimensions {\n"
                + "  key: \"cluster\"\n"
                + "  value: \"cluster-test\"\n"
                + "}\n",

                converted.get(0).toString());
    }
}
