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
                        .setValue(new DefaultQuantity.Builder().setValue(9999.0).setUnit(Unit.BIT).build())
                        .setIsSpecified(true)
                        .setPopulationSize((long) (1337)).build()))
                .setPeriod(Duration.ofMinutes(5))
                .setStart(ZonedDateTime.parse("2020-06-04T16:04:38.424-07:00[America/Los_Angeles]"))
                .build();

        final List<Messages.StatisticSetRecord> converted = PeriodicDataToProtoConverter.convert(data);

        Assert.assertEquals(1, converted.size());
        Assert.assertEquals(
                "service: \"service-test\"\n" +
                "cluster: \"cluster-test\"\n" +
                "metric: \"metric-test\"\n" +
                "period: \"PT5M\"\n" +
                "period_start: \"2020-06-04T16:04:38.424-07:00[America/Los_Angeles]\"\n" +
                "statistics {\n" +
                "  statistic: \"mean\"\n" +
                "  value: 1249.875\n" +
                "  unit: \"BYTE\"\n" +
                "  user_specified: true\n" +
                "}\n" +
                "dimensions {\n" +
                "  key: \"host\"\n" +
                "  value: \"host-test\"\n" +
                "}\n" +
                "dimensions {\n" +
                "  key: \"service\"\n" +
                "  value: \"service-test\"\n" +
                "}\n" +
                "dimensions {\n" +
                "  key: \"cluster\"\n" +
                "  value: \"cluster-test\"\n" +
                "}\n",

                converted.get(0).toString());
    }
}
