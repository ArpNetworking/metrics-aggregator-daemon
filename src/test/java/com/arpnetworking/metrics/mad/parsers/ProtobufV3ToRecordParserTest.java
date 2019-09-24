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
package com.arpnetworking.metrics.mad.parsers;

import akka.util.ByteString;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Tests for the V3 protobuf parser.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class ProtobufV3ToRecordParserTest {

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();

    @Test
    public void testParseEmpty() throws ParsingException, IOException {
        final List<Record> records = parseRecords("ProtobufV3ParserTest/testParseEmpty");

        Assert.assertEquals(0, records.size());
    }

    // CHECKSTYLE.OFF: MethodLength - It's a more complex record in v3.
    @Test
    public void testParseSingleRecord() throws ParsingException, IOException {
        HistogramStatistic.HistogramSupportingData supportingData;
        HistogramStatistic.HistogramSnapshot histogramSnapshot;
        CalculatedValue<?> calculatedValue;

        final UUID uuid = UUID.fromString("142949d2-c0fc-469e-9958-7d2be2c49fa5");
        final ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1513239602974L), ZoneOffset.UTC);

        final List<Record> records = parseRecords("ProtobufV3ParserTest/testSingleRecord");

        Assert.assertEquals(1, records.size());

        final Record record = records.get(0);

        Assert.assertEquals(uuid, UUID.fromString(record.getId()));
        Assert.assertEquals(time, record.getTime());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertEquals(2, record.getDimensions().size());
        Assert.assertEquals("North America", record.getDimensions().get("region"));
        Assert.assertEquals("Chrome", record.getDimensions().get("browser"));

        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(3, metrics.size());

        final Metric timer = metrics.get("timer1");
        Assert.assertNotNull(timer);
        Assert.assertEquals(MetricType.GAUGE, timer.getType());
        Assert.assertEquals(2, timer.getValues().size());
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(10.48d).build(), timer.getValues().get(0));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(108d).build(), timer.getValues().get(1));
        Assert.assertTrue(timer.getStatistics().isEmpty());

        final Metric histogram = metrics.get("histogram1");
        Assert.assertNotNull(histogram);
        Assert.assertEquals(MetricType.GAUGE, histogram.getType());
        Assert.assertEquals(0, histogram.getValues().size());
        Assert.assertEquals(5, histogram.getStatistics().size());
        for (final List<CalculatedValue<?>> values : histogram.getStatistics().values()) {
            Assert.assertEquals(1, values.size());
        }
        // Min
        calculatedValue = Iterables.getOnlyElement(
                histogram.getStatistics().get(STATISTIC_FACTORY.getStatistic("min")));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(1.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Max
        calculatedValue = Iterables.getOnlyElement(
                histogram.getStatistics().get(STATISTIC_FACTORY.getStatistic("max")));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(5.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Count
        calculatedValue = Iterables.getOnlyElement(
                histogram.getStatistics().get(STATISTIC_FACTORY.getStatistic("count")));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(9.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Sum
        calculatedValue = Iterables.getOnlyElement(
                histogram.getStatistics().get(STATISTIC_FACTORY.getStatistic("sum")));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(27.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Histogram
        calculatedValue = Iterables.getOnlyElement(
                histogram.getStatistics().get(STATISTIC_FACTORY.getStatistic("histogram")));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(1.0).build(), calculatedValue.getValue());
        Assert.assertTrue(calculatedValue.getData() instanceof HistogramStatistic.HistogramSupportingData);
        supportingData = (HistogramStatistic.HistogramSupportingData) calculatedValue.getData();
        histogramSnapshot = supportingData.getHistogramSnapshot();
        Assert.assertFalse(supportingData.getUnit().isPresent());
        Assert.assertEquals(9, histogramSnapshot.getEntriesCount());
        Assert.assertEquals(7, histogramSnapshot.getPrecision());
        Assert.assertEquals(1, histogramSnapshot.getValue(1.0));
        Assert.assertEquals(2, histogramSnapshot.getValue(2.0));
        Assert.assertEquals(3, histogramSnapshot.getValue(3.0));
        Assert.assertEquals(2, histogramSnapshot.getValue(4.0));
        Assert.assertEquals(1, histogramSnapshot.getValue(5.0));

        final Metric combined = metrics.get("combined1");
        Assert.assertNotNull(combined);
        Assert.assertEquals(MetricType.GAUGE, combined.getType());
        Assert.assertEquals(4, combined.getValues().size());
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(1.0).build(), combined.getValues().get(0));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(3.0).build(), combined.getValues().get(1));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(2.0).build(), combined.getValues().get(2));
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(4.0).build(), combined.getValues().get(3));
        Assert.assertEquals(5, combined.getStatistics().size());
        for (final List<CalculatedValue<?>> values : combined.getStatistics().values()) {
            Assert.assertEquals(2, values.size());
        }
        // Min
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("min")).get(0);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(2.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Max
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("max")).get(0);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(5.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Count
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("count")).get(0);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(4.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Sum
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("sum")).get(0);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(14.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Histogram
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("histogram")).get(0);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(1.0).build(), calculatedValue.getValue());
        Assert.assertTrue(calculatedValue.getData() instanceof HistogramStatistic.HistogramSupportingData);
        supportingData = (HistogramStatistic.HistogramSupportingData) calculatedValue.getData();
        histogramSnapshot = supportingData.getHistogramSnapshot();
        Assert.assertFalse(supportingData.getUnit().isPresent());
        Assert.assertEquals(4, histogramSnapshot.getEntriesCount());
        Assert.assertEquals(7, histogramSnapshot.getPrecision());
        Assert.assertEquals(1, histogramSnapshot.getValue(2.0));
        Assert.assertEquals(1, histogramSnapshot.getValue(3.0));
        Assert.assertEquals(1, histogramSnapshot.getValue(4.0));
        Assert.assertEquals(1, histogramSnapshot.getValue(5.0));
        // Min
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("min")).get(1);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(3.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Max
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("max")).get(1);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(3.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Count
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("count")).get(1);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(1.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Sum
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("sum")).get(1);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(3.0).build(), calculatedValue.getValue());
        Assert.assertNull(calculatedValue.getData());
        // Histogram
        calculatedValue = combined.getStatistics().get(STATISTIC_FACTORY.getStatistic("histogram")).get(1);
        Assert.assertEquals(new DefaultQuantity.Builder().setValue(1.0).build(), calculatedValue.getValue());
        Assert.assertTrue(calculatedValue.getData() instanceof HistogramStatistic.HistogramSupportingData);
        supportingData = (HistogramStatistic.HistogramSupportingData) calculatedValue.getData();
        histogramSnapshot = supportingData.getHistogramSnapshot();
        Assert.assertFalse(supportingData.getUnit().isPresent());
        Assert.assertEquals(1, histogramSnapshot.getEntriesCount());
        Assert.assertEquals(7, histogramSnapshot.getPrecision());
        Assert.assertEquals(1, histogramSnapshot.getValue(3.0));
    }
    // CHECKSTYLE.ON: MethodLength

    private static List<Record> parseRecords(final String fileName) throws ParsingException, IOException {
        return parseRecords(fileName, createParser());
    }

    private static List<Record> parseRecords(
            final String fileName,
            final Parser<List<Record>, HttpRequest> parser)
            throws ParsingException, IOException {
        final ByteString body =
                ByteString.fromArray(Resources.toByteArray(Resources.getResource(ProtobufV3ToRecordParserTest.class, fileName)));
        return parser.parse(new HttpRequest(ImmutableMultimap.of(), body));
    }

    private static Parser<List<Record>, HttpRequest> createParser() {
        return new ProtobufV3ToRecordParser();
    }
}
