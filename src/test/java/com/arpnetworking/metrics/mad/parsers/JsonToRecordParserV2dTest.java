/*
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
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.io.Resources;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Tests for the 2d version of the query log format.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class JsonToRecordParserV2dTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2dTest/testParse.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertNotNull(record.getAnnotations());

        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));

        final Map<String, ? extends Metric> variables = record.getMetrics();
        MatcherAssert.assertThat(variables, Matchers.hasKey("foo"));
        final Metric fooVar = variables.get("foo");
        Assert.assertEquals(4, fooVar.getValues().size());
        Assert.assertTrue(fooVar.getValues().get(0).getUnit().isPresent());
        Assert.assertEquals(Unit.SECOND, fooVar.getValues().get(0).getUnit().get());

        MatcherAssert.assertThat(variables, Matchers.hasKey("mem"));
        final Metric memVar = variables.get("mem");
        Assert.assertEquals(3, memVar.getValues().size());
        Assert.assertTrue(memVar.getValues().get(0).getUnit().isPresent());
        Assert.assertEquals(Unit.BYTE, memVar.getValues().get(0).getUnit().get());

        MatcherAssert.assertThat(variables, Matchers.hasKey("dbQueries"));
        final Metric queriesVar = variables.get("dbQueries");
        Assert.assertEquals(1, queriesVar.getValues().size());
        Assert.assertFalse(queriesVar.getValues().get(0).getUnit().isPresent());
    }

    @Test
    public void testAnnotations() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2dTest/testAnnotations.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertNotNull(record.getAnnotations());

        Assert.assertEquals(2, record.getAnnotations().size());
        MatcherAssert.assertThat(record.getAnnotations(), IsMapContaining.hasEntry("method", "POST"));
        MatcherAssert.assertThat(record.getAnnotations(), IsMapContaining.hasEntry("request_id", "c5251254-8f7c-4c21-95da-270eb66e100b"));

        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
    }

    @Test
    public void testNoValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2dTest/testNoValues.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertNotNull(record.getAnnotations());
        Assert.assertNotNull(record.getDimensions());

        final Map<String, ? extends Metric> variables = record.getMetrics();
        MatcherAssert.assertThat(variables, Matchers.hasKey("foo"));
        final Metric fooVar = variables.get("foo");
        Assert.assertEquals(0, fooVar.getValues().size());
    }

    @Test
    public void testNaNValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2dTest/testNaNValues.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertNotNull(record.getAnnotations());

        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));

        final Map<String, ? extends Metric> variables = record.getMetrics();
        Assert.assertEquals(3, variables.size());

        MatcherAssert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = variables.get("t1");
        Assert.assertTrue(t1.getValues().isEmpty());

        MatcherAssert.assertThat(variables, Matchers.hasKey("g1"));
        final Metric g1 = variables.get("g1");
        Assert.assertTrue(g1.getValues().isEmpty());

        MatcherAssert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = variables.get("c1");
        Assert.assertTrue(c1.getValues().isEmpty());
    }

    @Test(expected = ParsingException.class)
    public void testBadFinalTimestamp() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testBadFinalTimestamp.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingFinalTimestamp() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testMissingFinalTimestamp.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadInitialTimestamps() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testBadInitialTimestamp.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullAnnotations() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testNullAnnotations.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingAnnotations() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testMissingAnnotations.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingBothTimestamps() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testMissingBothTimestamps.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadAnnotations() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testBadAnnotations.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadElement() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testBadElement.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullElement() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testNullElement.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testNullValue.json");
    }

    @Test(expected = ParsingException.class)
    public void parse2dBadValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testBadValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullMetric() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2dTest/testNullMetric.json");
    }

    private static Record parseRecord(final String fileName) throws ParsingException, IOException {
        return new JsonToRecordParser.Builder()
                .setDefaultCluster("MyCluster")
                .setDefaultService("MyService")
                .setDefaultHost("MyHost")
                .build()
                .parse(Resources.toByteArray(Resources.getResource(JsonToRecordParserV2dTest.class, fileName)));
    }
}
