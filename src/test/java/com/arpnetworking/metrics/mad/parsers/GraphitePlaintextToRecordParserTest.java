/*
 * Copyright 2017 Inscope Metrics, Inc.
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

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Tests for the Graphite plaintext parser.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class GraphitePlaintextToRecordParserTest {

    private final Supplier<GraphitePlaintextToRecordParser.Builder> _graphiteParserBuilder =
            () -> new GraphitePlaintextToRecordParser.Builder()
                    .setGlobalTags(ImmutableMap.of("host", "localhost"))
                    .setParseCarbonTags(true);


    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _graphiteParserBuilder.get(),
                GraphitePlaintextToRecordParser.class);
    }

    @Test
    public void testToString() {
        final String asString = _graphiteParserBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testParseSingleLineNoLineEnding() throws ParsingException, IOException {
        final Record record = parseRecord("GraphitePlaintextParserTest/testParseSingleLineNoLineEnding");

        Assert.assertNotNull(record);

        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(0, record.getDimensions().size());

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(1, map.size());

        final Metric metric = map.get("foo.bar");
        final List<Quantity> vals = metric.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.GAUGE, metric.getType());

        Assert.assertEquals(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC),
                record.getTime());
    }

    @Test
    public void testParseSingleLineWithLineEnding() throws ParsingException, IOException {
        final Record record = parseRecord("GraphitePlaintextParserTest/testParseSingleLineWithLineEnding");

        Assert.assertNotNull(record);

        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(0, record.getDimensions().size());

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(1, map.size());

        final Metric metric = map.get("foo.bar");
        final List<Quantity> vals = metric.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.GAUGE, metric.getType());

        Assert.assertEquals(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC),
                record.getTime());
    }

    @Test
    public void tesParseSingleLineNoTimestamp() throws ParsingException, IOException {
        final ZonedDateTime before = ZonedDateTime.now();
        final Record record = parseRecord("GraphitePlaintextParserTest/testParseSingleLineNoTimestamp");
        final ZonedDateTime after = ZonedDateTime.now();

        Assert.assertNotNull(record);

        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(0, record.getDimensions().size());

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(1, map.size());

        final Metric metric = map.get("foo.bar");
        final List<Quantity> vals = metric.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.GAUGE, metric.getType());

        Assert.assertFalse(record.getTime().isBefore(before));
        Assert.assertFalse(record.getTime().isAfter(after));
    }

    @Test
    public void testGlobalTags() throws ParsingException, IOException {
        final Record record = parseRecord(
                "GraphitePlaintextParserTest/testParseSingleLineWithLineEnding",
                new GraphitePlaintextToRecordParser.Builder()
                        .setGlobalTags(ImmutableMap.of(
                                "cluster", "MyCluster",
                                "service", "MyService",
                                "host", "MyHost",
                                "region", "US",
                                "foo", "bar"))
                        .build());

        Assert.assertNotNull(record);

        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("US", record.getDimensions().get("region"));
        Assert.assertEquals("bar", record.getDimensions().get("foo"));

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(1, map.size());

        final Metric metric = map.get("foo.bar");
        final List<Quantity> vals = metric.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.GAUGE, metric.getType());

        Assert.assertEquals(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC),
                record.getTime());
    }

    @Test
    public void testCarbonTags() throws ParsingException, IOException {
        final Record record = parseRecord(
                "GraphitePlaintextParserTest/testParseCarbonTags",
                new GraphitePlaintextToRecordParser.Builder()
                        .setParseCarbonTags(true)
                        .build());

        Assert.assertNotNull(record);

        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("US", record.getDimensions().get("region"));
        Assert.assertEquals("bar", record.getDimensions().get("foo"));

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(1, map.size());

        final Metric metric = map.get("foo.bar");
        final List<Quantity> vals = metric.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.GAUGE, metric.getType());

        Assert.assertEquals(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC),
                record.getTime());
    }

    @Test
    public void testCarbonTagsOverrideGlobalTags() throws ParsingException, IOException {
        final Record record = parseRecord(
                "GraphitePlaintextParserTest/testParseCarbonTags",
                new GraphitePlaintextToRecordParser.Builder()
                        .setGlobalTags(ImmutableMap.of("region", "CA"))
                        .setParseCarbonTags(true)
                        .build());

        Assert.assertNotNull(record);

        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("US", record.getDimensions().get("region"));
        Assert.assertEquals("bar", record.getDimensions().get("foo"));

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(1, map.size());

        final Metric metric = map.get("foo.bar");
        final List<Quantity> vals = metric.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.GAUGE, metric.getType());

        Assert.assertEquals(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC),
                record.getTime());
    }

    private static Record parseRecord(final String fileName) throws ParsingException, IOException {
        return parseRecord(fileName, createParser());
    }

    private static Record parseRecord(
            final String fileName,
            final Parser<List<Record>, ByteBuffer> parser)
            throws ParsingException, IOException {
        return Iterables.getOnlyElement(parseRecords(fileName, parser));
    }

    private static List<Record> parseRecords(
            final String fileName,
            final Parser<List<Record>, ByteBuffer> parser)
            throws ParsingException, IOException {
        return parser.parse(ByteBuffer.wrap(
                Resources.toByteArray(
                        Resources.getResource(
                                GraphitePlaintextToRecordParserTest.class,
                                fileName))));
    }

    private static Parser<List<Record>, ByteBuffer> createParser() {
        return new GraphitePlaintextToRecordParser.Builder().build();
    }
}
