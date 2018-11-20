/*
 * Copyright 2018 Inscope Metrics, Inc.
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
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

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

    @Test
    public void testParseSingleRecord() throws ParsingException, IOException {
        final ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1542330080682L), ZoneOffset.UTC);
        final List<Record> records = parseRecords("PrometheusParserTest/testSingleRecord");

        Assert.assertEquals(1, records.size());

        final Record record = records.get(0);

        Assert.assertEquals(time, record.getTime());

        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());

        final Metric gauge = metrics.get("rpc_durations_histogram_seconds_count");
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

    @Test(expected = ParsingException.class)
    public void testCorruptedPrometheusMessage() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testCorruptedPrometheusMessage");
    }

    @Test(expected = ParsingException.class)
    public void testCorruptedSnappy() throws ParsingException, IOException {
        parseRecords("PrometheusParserTest/testCorruptedSnappy");
    }

    @Test
    public void testUnitParserEmpty() {
        Assert.assertEquals(Optional.empty(), createParser().parseUnit(Optional.empty()));
    }

    @Test
    public void testUnitParserInvalidUnit() {
        Assert.assertEquals(Optional.empty(), createParser().parseUnit(Optional.of("foo_bar")));
        Assert.assertEquals(Optional.empty(), createParser().parseUnit(Optional.of("foo_seconds_bar")));
        Assert.assertEquals(Optional.empty(), createParser().parseUnit(Optional.of("seconds_bar")));
    }

    @Test
    public void testUnitParserSeconds() {
        testUnitParsing("seconds", Optional.of(Unit.SECOND));
    }

    @Test
    public void testUnitParserCelcius() {
        testUnitParsing("celcius", Optional.of(Unit.CELCIUS));
    }

    @Test
    public void testUnitParserBytes() {
        testUnitParsing("bytes", Optional.of(Unit.BYTE));
    }

    @Test
    public void testUnitParserBits() {
        testUnitParsing("bits", Optional.of(Unit.BIT));
    }

    private static void testUnitParsing(final String name, final Optional<Unit> expected) {
        final PrometheusToRecordParser parser = createParser();
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name)));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of("foo_" + name)));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name + "_total")));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name + "_bucket")));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name + "_sum")));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name + "_avg")));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name + "_count")));
        Assert.assertEquals(expected, parser.parseUnit(Optional.of(name + "_total_count")));
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
        return new PrometheusToRecordParser();
    }
}
