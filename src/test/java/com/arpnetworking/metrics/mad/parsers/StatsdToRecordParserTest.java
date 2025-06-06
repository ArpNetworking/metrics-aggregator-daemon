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

import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Record;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.random.RandomGenerator;

/**
 * Tests for the statsd parser.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class StatsdToRecordParserTest {

    @Before
    public void setUp() {
        _mocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void after() throws Exception {
        _mocks.close();
    }

    @Test
    public void testExampleCounter() throws ParsingException {
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of())
                        .setMetrics(ImmutableMap.of(
                                "page.views",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("page.views:1|c".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    public void testExampleGauge() throws ParsingException {
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of())
                        .setMetrics(ImmutableMap.of(
                                "fuel.level",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(0.5)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("fuel.level:0.5|g".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    public void testExampleSamplingAccept() throws ParsingException {
        Mockito.doReturn(0.49).when(_random).nextDouble();
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of())
                        .setMetrics(ImmutableMap.of(
                                "song.length",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.TIMER)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(240.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("song.length:240|h|@0.5".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    public void testExampleSamplingReject() throws ParsingException {
        Mockito.doReturn(0.51).when(_random).nextDouble();
        Assert.assertTrue(_parser.parse(ByteBuffer.wrap("song.length:240|h|@0.5".getBytes(StandardCharsets.UTF_8))).isEmpty());
    }

    @Test
    public void testSamplingAlwaysAccept() throws ParsingException {
        Mockito.doReturn(0.0).when(_random).nextDouble();
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of())
                        .setMetrics(ImmutableMap.of(
                                "song.length",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.TIMER)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(240.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("song.length:240|h|@1".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    public void testSamplingAlwaysReject() throws ParsingException {
        Mockito.doReturn(0.0).when(_random).nextDouble();
        Assert.assertTrue(_parser.parse(ByteBuffer.wrap("song.length:240|h|@0".getBytes(StandardCharsets.UTF_8))).isEmpty());
    }

    @Test(expected = ParsingException.class)
    public void testExampleSetsNotSupported() throws ParsingException {
        _parser.parse(ByteBuffer.wrap("users.uniques:1234|s".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testExampleTags() throws ParsingException {
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of(
                                "country", "china"))
                        .setMetrics(ImmutableMap.of(
                                "users.online",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("users.online:1|c|#country:china".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    public void testExampleTagsSampledAccept() throws ParsingException {
        Mockito.doReturn(0.49).when(_random).nextDouble();
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of(
                                "country", "china"))
                        .setMetrics(ImmutableMap.of(
                                "users.online",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("users.online:1|c|@0.5|#country:china".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test(expected = ParsingException.class)
    public void testExampleTagsInvalid() throws ParsingException {
        Mockito.doReturn(0.50).when(_random).nextDouble();
        _parser.parse(ByteBuffer.wrap("users.online:1|c|@0.5|#country:china,anotherTag".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testExampleTagsSampledReject() throws ParsingException {
        Mockito.doReturn(0.51).when(_random).nextDouble();
        Assert.assertTrue(
                _parser.parse(ByteBuffer.wrap("users.online:1|c|@0.5|#country:china".getBytes(StandardCharsets.UTF_8))).isEmpty());
    }

    @Test
    public void testInfluxStyleTagFormat() throws ParsingException {
        assertRecordEquality(
                new DefaultRecord.Builder()
                        .setTime(_now)
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(ImmutableMap.of())
                        .setDimensions(ImmutableMap.of(
                                "service", "statsd"))
                        .setMetrics(ImmutableMap.of(
                                "users.online",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.COUNTER)
                                        .setValues(ImmutableList.of(
                                                new DefaultQuantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("users.online,service=statsd:1|c".getBytes(StandardCharsets.UTF_8)))));

    }

    @Test(expected = ParsingException.class)
    public void testInfluxStyleTagFormatInvalid() throws ParsingException {
        Mockito.doReturn(0.52).when(_random).nextDouble();
        _parser.parse(ByteBuffer.wrap("users.online,service=statsd,tag2:1|c".getBytes(StandardCharsets.UTF_8)));

    }

    @Test(expected = ParsingException.class)
    public void testInfluxStyleTagFormatInvalid2() throws ParsingException {
        Mockito.doReturn(0.53).when(_random).nextDouble();
        _parser.parse(ByteBuffer.wrap("users.online,:,service=statsd|c".getBytes(StandardCharsets.UTF_8)));
    }

    private void assertRecordEquality(final Record expected, final Record actual) {
        Assert.assertEquals(expected.getTime(), actual.getTime());
        Assert.assertEquals(expected.getAnnotations(), actual.getAnnotations());
        Assert.assertEquals(expected.getDimensions(), actual.getDimensions());
        Assert.assertEquals(expected.getMetrics(), actual.getMetrics());
    }

    @Mock
    private RandomGenerator _random;
    private AutoCloseable _mocks;

    private final ZonedDateTime _now = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS);
    private final Parser<List<Record>, ByteBuffer> _parser =
            new StatsdToRecordParser(
                    Clock.fixed(
                            _now.toInstant(),
                            ZoneId.of("UTC")),
                    () -> _random);
}
