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
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Tests for the statsd parser.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class StatsdToRecordParserTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
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
                                                new Quantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("page.views:1|c".getBytes(Charsets.UTF_8)))));
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
                                                new Quantity.Builder()
                                                        .setValue(0.5)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("fuel.level:0.5|g".getBytes(Charsets.UTF_8)))));
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
                                                new Quantity.Builder()
                                                        .setValue(240.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("song.length:240|h|@0.5".getBytes(Charsets.UTF_8)))));
    }

    @Test
    public void testExampleSamplingReject() throws ParsingException {
        Mockito.doReturn(0.51).when(_random).nextDouble();
        Assert.assertTrue(_parser.parse(ByteBuffer.wrap("song.length:240|h|@0.5".getBytes(Charsets.UTF_8))).isEmpty());
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
                                                new Quantity.Builder()
                                                        .setValue(240.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("song.length:240|h|@1".getBytes(Charsets.UTF_8)))));
    }

    @Test
    public void testSamplingAlwaysReject() throws ParsingException {
        Mockito.doReturn(0.0).when(_random).nextDouble();
        Assert.assertTrue(_parser.parse(ByteBuffer.wrap("song.length:240|h|@0".getBytes(Charsets.UTF_8))).isEmpty());
    }

    @Test(expected = ParsingException.class)
    public void testExampleSetsNotSupported() throws ParsingException {
        _parser.parse(ByteBuffer.wrap("users.uniques:1234|s".getBytes(Charsets.UTF_8)));
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
                                                new Quantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("users.online:1|c|#country:china".getBytes(Charsets.UTF_8)))));
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
                                                new Quantity.Builder()
                                                        .setValue(1.0)
                                                        .build()))
                                        .build()
                        ))
                        .build(),
                Iterables.getOnlyElement(
                        _parser.parse(ByteBuffer.wrap("users.online:1|c|@0.5|#country:china".getBytes(Charsets.UTF_8)))));
    }

    @Test
    public void testExampleTagsSampledReject() throws ParsingException {
        Mockito.doReturn(0.51).when(_random).nextDouble();
        Assert.assertTrue(_parser.parse(ByteBuffer.wrap("users.online:1|c|@0.5|#country:china".getBytes(Charsets.UTF_8))).isEmpty());
    }

    private void assertRecordEquality(final Record expected, final Record actual) {
        Assert.assertEquals(expected.getTime(), actual.getTime());
        Assert.assertEquals(expected.getAnnotations(), actual.getAnnotations());
        Assert.assertEquals(expected.getDimensions(), actual.getDimensions());
        Assert.assertEquals(expected.getMetrics(), actual.getMetrics());
    }

    @Mock
    private Random _random;

    private final ZonedDateTime _now = ZonedDateTime.now(ZoneOffset.UTC);
    private final Parser<List<Record>, ByteBuffer> _parser =
            new StatsdToRecordParser(
                    Clock.fixed(
                            _now.toInstant(),
                            ZoneId.of("UTC")),
                    () -> _random);
}
