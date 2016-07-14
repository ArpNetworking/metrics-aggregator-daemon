/**
 * Copyright 2014 Groupon.com
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

import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests for the 2c version of the query log format.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class JsonToRecordParserV2cTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testParse.json");

        Assert.assertNotNull(record);
        Assert.assertEquals("MyCluster", record.getAnnotations().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getAnnotations().get(Key.HOST_DIMENSION_KEY));

        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(5, map.size());

        final Metric bestForTimer = map.get("/incentive/bestfor");
        List<Quantity> vals = bestForTimer.getValues();
        Assert.assertEquals(2, vals.size());
        Assert.assertEquals(2070d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(1844d, vals.get(1).getValue(), 0.001);
        Assert.assertEquals(MetricType.TIMER, bestForTimer.getType());

        final Metric counter1Var = map.get("counter1");
        vals = counter1Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(7d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter1Var.getType());

        final Metric counter2Var = map.get("counter2");
        vals = counter2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter2Var.getType());

        final Metric gauge1Var = map.get("gauge1");
        vals = gauge1Var.getValues();
        Assert.assertEquals(2, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(2d, vals.get(1).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge1Var.getType());

        final Metric gauge2Var = map.get("gauge2");
        vals = gauge2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(15d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge2Var.getType());

        Assert.assertEquals(new DateTime((long) (1347527687.686 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
    }

    @Test
    public void testAnnotations() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testAnnotations.json");

        Assert.assertNotNull(record);
        Assert.assertEquals(5, record.getAnnotations().size());
        Assert.assertEquals("MyHost", record.getAnnotations().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertThat(record.getAnnotations(), IsMapContaining.hasEntry("method", "POST"));
        Assert.assertThat(record.getAnnotations(), IsMapContaining.hasEntry("request_id", "c5251254-8f7c-4c21-95da-270eb66e100b"));
    }

    @Test
    public void testMissingFinalTimestampFallback() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testMissingFinalTimestampFallback.json");

        Assert.assertNotNull(record);
        Assert.assertEquals(new DateTime((long) (1347527680.486 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
    }

    @Test
    public void testBadTimestampFallback() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testBadTimestampFallback.json");

        Assert.assertNotNull(record);
        Assert.assertEquals(new DateTime((long) (1347527680.486 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
    }

    @Test(expected = ParsingException.class)
    public void testBothTimestampsBad() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2cTest/testBothTimestampsBad.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounters() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2cTest/testBadCounters.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadAnnotations() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2cTest/testBadAnnotations.json");
    }

    @Test
    public void testBadValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testBadValues.json");

        Assert.assertNotNull(record);
        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(5, map.size());

        final Metric bestForTimer = map.get("/incentive/bestfor");
        List<Quantity> vals = bestForTimer.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(2070d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.TIMER, bestForTimer.getType());

        final Metric counter1Var = map.get("counter1");
        vals = counter1Var.getValues();
        Assert.assertEquals(0, vals.size());
        Assert.assertEquals(MetricType.COUNTER, counter1Var.getType());

        final Metric counter2Var = map.get("counter2");
        vals = counter2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter2Var.getType());

        final Metric gauge1Var = map.get("gauge1");
        vals = gauge1Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge1Var.getType());

        final Metric gauge2Var = map.get("gauge2");
        vals = gauge2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(15d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge2Var.getType());

        Assert.assertEquals(new DateTime((long) (1347527687.686 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
    }

    @Test
    public void testMissingCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testMissingCounters.json");

        Assert.assertNotNull(record);
        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(3, map.size());

        final Metric bestForTimer = map.get("/incentive/bestfor");
        List<Quantity> vals = bestForTimer.getValues();
        Assert.assertEquals(2, vals.size());
        Assert.assertEquals(2070d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(1844d, vals.get(1).getValue(), 0.001);
        Assert.assertEquals(MetricType.TIMER, bestForTimer.getType());

        final Metric gauge1Var = map.get("gauge1");
        vals = gauge1Var.getValues();
        Assert.assertEquals(2, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(2d, vals.get(1).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge1Var.getType());

        final Metric gauge2Var = map.get("gauge2");
        vals = gauge2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(15d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge2Var.getType());

        Assert.assertEquals(new DateTime((long) (1347527687.686 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
    }

    @Test
    public void testMissingTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testMissingTimers.json");

        Assert.assertNotNull(record);
        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(4, map.size());

        final Metric counter1Var = map.get("counter1");
        List<Quantity> vals = counter1Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(7d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter1Var.getType());

        final Metric counter2Var = map.get("counter2");
        vals = counter2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter2Var.getType());

        final Metric gauge1Var = map.get("gauge1");
        vals = gauge1Var.getValues();
        Assert.assertEquals(2, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(2d, vals.get(1).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge1Var.getType());

        final Metric gauge2Var = map.get("gauge2");
        vals = gauge2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(15d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.GAUGE, gauge2Var.getType());

        final DateTime timestamp = record.getTime();
        Assert.assertEquals(new DateTime((long) (1347527687.686 * 1000d), ISOChronology.getInstanceUTC()), timestamp);
    }

    @Test
    public void testMissingGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testMissingGauges.json");

        Assert.assertNotNull(record);
        final Map<String, ? extends Metric> map = record.getMetrics();
        Assert.assertEquals(3, map.size());

        final Metric bestForTimer = map.get("/incentive/bestfor");
        List<Quantity> vals = bestForTimer.getValues();
        Assert.assertEquals(2, vals.size());
        Assert.assertEquals(2070d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(1844d, vals.get(1).getValue(), 0.001);
        Assert.assertEquals(MetricType.TIMER, bestForTimer.getType());

        final Metric counter1Var = map.get("counter1");
        vals = counter1Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(7d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter1Var.getType());

        final Metric counter2Var = map.get("counter2");
        vals = counter2Var.getValues();
        Assert.assertEquals(1, vals.size());
        Assert.assertEquals(1d, vals.get(0).getValue(), 0.001);
        Assert.assertEquals(MetricType.COUNTER, counter2Var.getType());

        Assert.assertEquals(new DateTime((long) (1347527687.686 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
    }

    @Test(expected = ParsingException.class)
    public void testMissingAnnotations() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2cTest/testMissingAnnotations.json");
    }

    @Test
    public void testNaNValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2cTest/testNaNValues.json");
        Assert.assertNotNull(record);

        Assert.assertEquals(new DateTime((long) (1347527687.686 * 1000d), ISOChronology.getInstanceUTC()), record.getTime());
        Assert.assertEquals(3, record.getAnnotations().size());
        Assert.assertEquals("MyHost", record.getAnnotations().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get(Key.CLUSTER_DIMENSION_KEY));

        final Map<String, ? extends Metric> variables = record.getMetrics();
        Assert.assertEquals(3, variables.size());

        Assert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = variables.get("t1");
        Assert.assertTrue(t1.getValues().isEmpty());

        Assert.assertThat(variables, Matchers.hasKey("g1"));
        final Metric g1 = variables.get("g1");
        Assert.assertTrue(g1.getValues().isEmpty());

        Assert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = variables.get("c1");
        Assert.assertTrue(c1.getValues().isEmpty());
    }

    @Test
    public void testDefaultHostname() throws Exception {
        final Record record = new JsonToRecordParser.Builder()
                .setDefaultCluster("MyCluster")
                .setDefaultService("MyService")
                .build()
                .parse(Resources.toByteArray(
                        Resources.getResource(JsonToRecordParserV2cTest.class, "QueryLogParserV2cTest/testDefaultHostname.json")));
        Assert.assertFalse(Strings.isNullOrEmpty(record.getAnnotations().get(Key.HOST_DIMENSION_KEY)));

    }

    private static Record parseRecord(final String fileName) throws ParsingException, IOException {
        return new JsonToRecordParser.Builder()
                .setDefaultCluster("MyCluster")
                .setDefaultService("MyService")
                .setDefaultHost("MyHost")
                .build()
                .parse(Resources.toByteArray(
                        Resources.getResource(JsonToRecordParserV2cTest.class, fileName)));
    }
}
