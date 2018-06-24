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
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.io.Resources;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for the 2e version of the query log format.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class JsonToRecordParserV2eTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testParse.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));


        final Map<String, ? extends Metric> variables = record.getMetrics();
        Assert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = variables.get("t1");
        Assert.assertEquals(4, t1.getValues().size());
        assertValue(t1.getValues().get(0), 1d, Unit.SECOND);
        assertValue(t1.getValues().get(1), 0.002d, Unit.SECOND);
        assertValue(t1.getValues().get(2), 0d, Unit.SECOND);
        assertValue(t1.getValues().get(3), 4d, Unit.SECOND);

        Assert.assertThat(variables, Matchers.hasKey("t2"));
        final Metric t2 = variables.get("t2");
        Assert.assertEquals(3, t2.getValues().size());
        assertValue(t2.getValues().get(0), 5d);
        assertValue(t2.getValues().get(1), 6d);
        assertValue(t2.getValues().get(2), 4d);

        Assert.assertThat(variables, Matchers.hasKey("g1"));
        final Metric g1 = variables.get("g1");
        Assert.assertEquals(3, g1.getValues().size());
        assertValue(g1.getValues().get(0), 1.2E9d, Unit.BYTE);
        assertValue(g1.getValues().get(1), 1.1d);
        assertValue(g1.getValues().get(2), 0.8d, Unit.SECOND);

        Assert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = variables.get("c1");
        Assert.assertEquals(1, c1.getValues().size());
        assertValue(c1.getValues().get(0), 1d);
    }

    @Test
    public void testEmpty() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testEmpty.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testNullCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testNullCounters.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testNullTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testNullTimers.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testNullGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testNullGauges.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testEmptyValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testEmptyValues.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertNotNull(record.getAnnotations());
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));

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
    public void testNaNValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testNaNValues.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertNotNull(record.getAnnotations());

        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));

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
    public void testUpperCaseVersion() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testUpperCaseVersion.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testContainerMissingContext() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testContainerMissingContext.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testContainerNullContext() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testContainerNullContext.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testContainerNullId() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testContainerNullId.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testContainerMissingId() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testContainerMissingId.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testMissingCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testMissingCounters.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testMissingTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testMissingTimers.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testMissingGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2eTest/testMissingGauges.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test(expected = ParsingException.class)
    public void testContainerMissingData() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerMissingData.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerMissingLevel() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerMissingLevel.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerMissingName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerMissingName.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerMissingTime() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerMissingTime.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerNullData() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerNullData.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerNullLevel() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerNullLevel.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerNullName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerNullName.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerNullTime() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerNullTime.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerBadData() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerBadData.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerBadLevel() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerBadLevel.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerBadName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerBadName.json");
    }

    @Test(expected = ParsingException.class)
    public void testContainerBadTime() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testContainerBadTime.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounters() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadCounters.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimers() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadTimers.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGauges() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadGauges.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testMissingGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testMissingTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testMissingCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testNullGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testNullTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testNullCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingCounterValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testMissingCounterValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingGaugeValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testMissingGaugeValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimerValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testMissingTimerValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadTimerValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadGaugeValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2eTest/testBadCounterValuesValue.json");
    }

    private static void assertValue(final Quantity quantity, final double value) {
        assertValue(quantity, value, Optional.<Unit>empty());
    }

    private static void assertValue(final Quantity quantity, final double value, final Unit unit) {
        assertValue(quantity, value, Optional.of(unit));
    }

    private static void assertValue(final Quantity quantity, final double value, final Optional<Unit> unit) {
        Assert.assertEquals(value, quantity.getValue(), 0.001);
        if (unit.isPresent()) {
            Assert.assertTrue(quantity.getUnit().isPresent());
            Assert.assertEquals(unit.get(), quantity.getUnit().get());
        } else {
            Assert.assertFalse(quantity.getUnit().isPresent());
        }
    }

    private static Record parseRecord(final String fileName) throws ParsingException, IOException {
        return new JsonToRecordParser.Builder()
                .setDefaultCluster("MyCluster")
                .setDefaultService("MyService")
                .setDefaultHost("MyHost")
                .build()
                .parse(Resources.toByteArray(Resources.getResource(JsonToRecordParserV2eTest.class, fileName)));
    }
}
