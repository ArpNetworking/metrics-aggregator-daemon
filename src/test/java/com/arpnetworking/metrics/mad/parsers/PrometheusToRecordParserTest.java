/*
 * Copyright 2018 Bruno Green.
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
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Tests for the prometheus parser.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
public final class PrometheusToRecordParserTest {

    @Test
    public void testParseEmpty() throws ParsingException, IOException {
        final List<Record> records = parseRecords("PrometheusParserTest/testParseEmpty");

        Assert.assertEquals(0, records.size());
    }

    @Test()
    @Ignore("Currently dropping aggregates")
    public void testParseSingleRecord() throws ParsingException, IOException {
        final ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1542330080682L), ZoneOffset.UTC);
        final List<Record> records = parseRecords("PrometheusParserTest/testSingleRecord");

        Assert.assertEquals(1, records.size());

        final Record record = records.get(0);

        Assert.assertEquals(time, record.getTime());

        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());

        final Metric gauge = metrics.get("rpc_durations_histogram_count");
        Assert.assertNotNull(gauge);
        Assert.assertEquals(MetricType.GAUGE, gauge.getType());
        Assert.assertEquals(1, gauge.getValues().size());
        final Quantity gaugeQuantity = gauge.getValues().get(0);
        Assert.assertEquals(Optional.of(Unit.SECOND), gaugeQuantity.getUnit());
        Assert.assertEquals(493.0, gaugeQuantity.getValue(), 0.001);

        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertEquals(4, record.getDimensions().size());
        Assert.assertEquals("production", record.getDimensions().get("group"));
        Assert.assertEquals("localhost:12345", record.getDimensions().get("instance"));
        Assert.assertEquals("example-random", record.getDimensions().get("job"));
        Assert.assertEquals("codelab-monitor", record.getDimensions().get("monitor"));
    }

    @Test
    public void testParseSingleRecordWithoutUnitInterpreter() throws ParsingException, IOException {
        final List<Record> records = parseRecords("PrometheusParserTest/testLivePrometheus2", createParserWithoutInterpreter());
        final Record record = records.get(57);
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric gauge = metrics.get("container_spec_memory_limit_bytes");
        Assert.assertNotNull(gauge);
        Assert.assertEquals(MetricType.GAUGE, gauge.getType());
        Assert.assertEquals(1, gauge.getValues().size());
        final Quantity gaugeQuantity = gauge.getValues().get(0);
        Assert.assertEquals(Optional.empty(), gaugeQuantity.getUnit());
        Assert.assertEquals(1.3500524544e11, gaugeQuantity.getValue(), 0.001);
    }

    @Test(expected = ParsingException.class)
    public void testCorruptedPrometheusMessage() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testCorruptedPrometheusMessage");
    }

    @Test(expected = ParsingException.class)
    public void testCorruptedSnappy() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testCorruptedSnappy");
    }

    @Test(expected = ParsingException.class)
    public void testMetricNoName() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testNoNameMetric");
    }

    @Test(expected = ParsingException.class)
    public void testEmptyName() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testEmptyNameMetric");
    }

    @Test(expected = ParsingException.class)
    public void testWhitespaceName() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testWhitespaceNameMetric");
    }

    @Test
    public void testUnitParserNoUnit() {
        testUnitParserNoUnitHelper("foo_bar");
        testUnitParserNoUnitHelper("foo_seconds_bar");
        testUnitParserNoUnitHelper("seconds_bar");
        testUnitParserNoUnitHelper("foo_seconds_total_");
        testUnitParserNoUnitHelper("foo_seconds_");
    }
    private void testUnitParserNoUnitHelper(final String name) {
        final PrometheusToRecordParser.ParseResult expectedResult
                = new PrometheusToRecordParser.ParseResult(name, Optional.empty(), Optional.empty());
        Assert.assertEquals(expectedResult, createParser().parseNameAndUnit(name));
    }

    @Test
    public void testUnitParserSeconds() {
        testUnitParsing("seconds", Unit.SECOND);
    }

    @Test
    public void testUnitParserCelcius() {
        testUnitParsing("celcius", Unit.CELCIUS);
    }

    @Test
    public void testUnitParserBytes() {
        testUnitParsing("bytes", Unit.BYTE);
    }

    @Test
    public void testUnitParserBits() {
        testUnitParsing("bits", Unit.BIT);
    }

    @Test
    public void testLive1() throws ParsingException, IOException {
        final List<Record> records = parseRecords("PrometheusParserTest/testLivePrometheus1");

        Assert.assertEquals(294, records.size());
    }

    private static void testUnitParsing(final String prometheusUnit, final Unit expected) {
        assertUnitNewName(prometheusUnit, expected, null);
        assertUnitNewName("foo_" + prometheusUnit, expected, null);
        assertUnitNewName("foo_" + prometheusUnit + "_total", expected, "total");
        assertUnitNewName("foo_" + prometheusUnit + "_bucket", expected, "bucket");
        assertUnitNewName("foo_" + prometheusUnit + "_sum", expected, "sum");
        assertUnitNewName("foo_" + prometheusUnit + "_avg", expected, "avg");
        assertUnitNewName("foo_" + prometheusUnit + "_count", expected, "count");
        assertUnitNewName(prometheusUnit + "_total_count", null, "count");
        assertUnitNewName("foo_" + prometheusUnit + "_total_count", null, "count");
        assertUnitNewName("foo_" + prometheusUnit + "_total", expected, "total");
        assertUnitNewName("foo_" + prometheusUnit + "_sum", expected, "sum");
    }
    private static void assertUnitNewName(
            final String fullName,
            @Nullable final Unit expectedUnit,
            @Nullable  final String expectedAggregation) {
        final PrometheusToRecordParser parser = createParser();
        final PrometheusToRecordParser.ParseResult expectedResult
                = new PrometheusToRecordParser.ParseResult(
                        fullName, Optional.ofNullable(expectedAggregation), Optional.ofNullable(expectedUnit));
        Assert.assertEquals(expectedResult, parser.parseNameAndUnit(fullName));
    }

    private static List<Record> parseRecords(final String fileName) throws ParsingException, IOException {
        return parseRecords(fileName, createParser());
    }

    private static List<Record> parseRecords(
            final String fileName,
            final Parser<List<Record>, HttpRequest> parser)
            throws ParsingException, IOException {
        final ByteString body =
                ByteString.fromArray(Resources.toByteArray(Resources.getResource(PrometheusToRecordParserTest.class, fileName)));
        return parser.parse(new HttpRequest(ImmutableMultimap.of(), body));
    }

    private static PrometheusToRecordParser createParser() {
        return new PrometheusToRecordParser(true, false);
    }
    private static PrometheusToRecordParser createParserWithoutInterpreter() {
        return new PrometheusToRecordParser(false, false);
    }
}
