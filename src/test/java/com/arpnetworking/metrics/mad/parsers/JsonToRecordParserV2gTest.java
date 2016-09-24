/**
 * Copyright 2016 Groupon.com
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
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for the 2g version of the query log format.
 *
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public class JsonToRecordParserV2gTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        // TODO(vkoskela): Test compound units [MAI-679]

        final Record record = parseRecord("QueryLogParserV2gTest/testParse.json");
        Assert.assertNotNull(record);
        Assert.assertEquals("6be33313-bb39-423a-a928-1d0cc0da60a9", record.getId());
        Assert.assertEquals(DateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertEquals("bar", record.getAnnotations().get("foo"));

        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("US", record.getDimensions().get("region"));

        final Map<String, ? extends Metric> variables = record.getMetrics();
        Assert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = variables.get("t1");
        Assert.assertEquals(4, t1.getValues().size());
        assertValue(t1.getValues().get(0), 1d, Unit.SECOND);
        assertValue(t1.getValues().get(1), 2d, Unit.MILLISECOND);
        assertValue(t1.getValues().get(2), 0d, Unit.MILLISECOND);
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
        assertValue(g1.getValues().get(0), 1.2d, Unit.GIGABYTE);
        assertValue(g1.getValues().get(1), 1.1d);
        assertValue(g1.getValues().get(2), 0.8d, Unit.SECOND);

        Assert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = variables.get("c1");
        Assert.assertEquals(1, c1.getValues().size());
        assertValue(c1.getValues().get(0), 1d);
    }

    @Test
    public void testEmpty() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testEmpty.json");
        Assert.assertNotNull(record);
        Assert.assertEquals("6be33313-bb39-423a-a928-1d0cc0da60a9", record.getId());
        Assert.assertEquals(DateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertTrue(record.getAnnotations().isEmpty());

        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test 
    public void testPresentDimensions() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testPresentDimensions.json");
        Assert.assertNotNull(record);
        Assert.assertEquals("6be33313-bb39-423a-a928-1d0cc0da60a9", record.getId());
        Assert.assertEquals(DateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertTrue(record.getAnnotations().isEmpty());

        Assert.assertNotNull(record.getDimensions());
        Assert.assertEquals(5, record.getDimensions().size());
        Assert.assertEquals("DimVal1", record.getDimensions().get("Dim1"));
        Assert.assertEquals("DimVal2", record.getDimensions().get("Dim2"));
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
    }

    @Test(expected = ParsingException.class)
    public void testMissingDimensions() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingDimensions.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingId() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingId.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimestamp() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingTimeStamp.json");
    }

    @Test(expected = ParsingException.class)
    public void testEmptyId() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testEmptyId.json");
    }

    @Test(expected = ParsingException.class)
    public void testEmptyDate() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testEmptyDate.json");
    }

    @Test
    public void testNullCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testNullCounters.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testNullTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testNullTimers.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testNullGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testNullGauges.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testMissingCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testMissingCounters.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testMissingTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testMissingTimers.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testMissingGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testMissingGauges.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testEmptyValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testEmptyValues.json");
        Assert.assertNotNull(record);
        Assert.assertEquals("6be33313-bb39-423a-a928-1d0cc0da60a9", record.getId());
        Assert.assertEquals(DateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertTrue(record.getAnnotations().isEmpty());

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
        final Record record = parseRecord("QueryLogParserV2gTest/testUpperCaseVersion.json");
        Assert.assertNotNull(record);
    }

    @Test(expected = ParsingException.class)
    public void testBadCounters() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounters.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimers() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimers.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGauges() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGauges.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testNullGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testNullTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testNullCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingCounterValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingCounterValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingGaugeValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingGaugeValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimerValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testMissingTimerValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimerValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGaugeValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounterValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueNumeratorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounterValuesValueNumeratorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueDenominatorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounterValuesValueDenominatorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueNumeratorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGaugeValuesValueNumeratorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueDenominatorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGaugeValuesValueDenominatorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueNumeratorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimerValuesValueNumeratorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueDenominatorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimerValuesValueDenominatorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueNumeratorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounterValuesValueNumeratorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueDenominatorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadCounterValuesValueDenominatorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueNumeratorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGaugeValuesValueNumeratorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueDenominatorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadGaugeValuesValueDenominatorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueNumeratorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimerValuesValueNumeratorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueDenominatorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2gTest/testBadTimerValuesValueDenominatorUnitsName.json");
    }

    @Test
    public void testNaNValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2gTest/testNaNValues.json");
        Assert.assertNotNull(record);
        Assert.assertEquals("6be33313-bb39-423a-a928-1d0cc0da60a9", record.getId());
        Assert.assertEquals(DateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        Assert.assertTrue(record.getAnnotations().isEmpty());

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

    private static void assertValue(final Quantity quantity, final double value) {
        assertValue(quantity, value, Optional.empty());
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
                .build()
                .parse(Resources.toByteArray(Resources.getResource(
                        JsonToRecordParserV2gTest.class, fileName)));
    }
}
