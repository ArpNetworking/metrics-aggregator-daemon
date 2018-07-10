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

package com.inscopemetrics.mad.parsers;

import com.google.common.io.Resources;
import com.inscopemetrics.mad.model.Key;
import com.inscopemetrics.mad.model.Metric;
import com.inscopemetrics.mad.model.MetricType;
import com.inscopemetrics.mad.model.Quantity;
import com.inscopemetrics.mad.model.Record;
import com.inscopemetrics.mad.parsers.exceptions.ParsingException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Tests for Telegraf JSON format.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class TelegrafJsonToRecordParserTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        final Collection<Record> records = parseRecord("TelegrafJsonParserTest/testParse.json");

        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());

        final Record record = records.iterator().next();

        verifyRecordOne(record);
    }

    @Test
    public void testParseBatch() throws ParsingException, IOException {
        final Collection<Record> records = parseRecord("TelegrafJsonParserTest/testBatch.json");

        Assert.assertNotNull(records);
        Assert.assertEquals(2, records.size());

        final Iterator<Record> iterator = records.iterator();
        final Record recordOne = iterator.next();
        final Record recordTwo = iterator.next();

        verifyRecordOne(recordOne);
        verifyRecordTwo(recordTwo);
    }

    @Test
    public void tesBlankName() throws ParsingException, IOException {
        final Collection<Record> records = parseRecord("TelegrafJsonParserTest/testBlankName.json");

        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());

        final Record record = records.iterator().next();

        Assert.assertNotNull(record);

        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));

        Assert.assertEquals(0, record.getAnnotations().size());

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(2, map.size());

        final Metric t1 = map.get("foo.t1");
        List<Quantity> vals = t1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(123d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, t1.getType());

        final Metric t2 = map.get("bar.t2");
        vals = t2.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, t2.getType());
    }


    private void verifyRecordOne(final Record record) {
        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("US", record.getDimensions().get("region"));
        Assert.assertEquals("bar", record.getDimensions().get("foo"));

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(5, map.size());

        final Metric t1 = map.get("MyName.t1");
        List<Quantity> vals = t1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(123d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, t1.getType());

        final Metric t2 = map.get("MyName.t2");
        vals = t2.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1.23d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, t2.getType());

        final Metric g1 = map.get("MyName.g1");
        vals = g1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(246d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, g1.getType());

        final Metric g2 = map.get("MyName.g2");
        vals = g2.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(2.46d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, g2.getType());

        final Metric c1 = map.get("MyName.c1");
        vals = c1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, c1.getType());

        Assert.assertEquals(ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC), record.getTime());
    }

    private void verifyRecordTwo(final Record record) {
        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(0, record.getAnnotations().size());

        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals("MyCluster2", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService2", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost2", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("CA", record.getDimensions().get("region"));
        Assert.assertEquals("foo", record.getDimensions().get("bar"));

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(5, map.size());

        final Metric t1 = map.get("MyName2.t1");
        List<Quantity> vals = t1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(456d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, t1.getType());

        final Metric t2 = map.get("MyName2.t2");
        vals = t2.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(4.56d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, t2.getType());

        final Metric g1 = map.get("MyName2.g1");
        vals = g1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(482d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, g1.getType());

        final Metric g2 = map.get("MyName2.g2");
        vals = g2.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(4.82d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, g2.getType());

        final Metric c1 = map.get("MyName2.c1");
        vals = c1.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(2d, vals.get(0).getValue(), 0.001);
        Assert.assertFalse(vals.get(0).getUnit().isPresent());
        Assert.assertEquals(MetricType.TIMER, c1.getType());

        Assert.assertEquals(ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) (1458229140 * 1000d)), ZoneOffset.UTC), record.getTime());
    }

    private static Collection<Record> parseRecord(final String fileName) throws ParsingException, IOException {
        return new TelegrafJsonToRecordParser.Builder()
                .build()
                .parse(ByteBuffer.wrap(
                        Resources.toByteArray(
                                Resources.getResource(
                                        TelegrafJsonToRecordParserTest.class,
                                        fileName))));
    }
}
