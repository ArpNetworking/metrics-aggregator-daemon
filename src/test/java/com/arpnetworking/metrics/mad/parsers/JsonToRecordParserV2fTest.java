/*
 * Copyright 2015 Groupon.com
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
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.io.Resources;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Tests for the 2f version of the query log format.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class JsonToRecordParserV2fTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        // TODO(vkoskela): Test compound units [MAI-679]

        final Record record = parseRecord("QueryLogParserV2fTest/testParse.json");
        Assert.assertNotNull(record);
        Assert.assertEquals(3, record.getAnnotations().size());
        Assert.assertEquals("MyHost", record.getAnnotations().get("_" + Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get("_" + Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get("_" + Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals("6be33313-bb39-423a-a928-1d0cc0da60a9", record.getId());
        Assert.assertFalse(record.getId().isEmpty());

        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());

        final Map<String, ? extends Metric> variables = record.getMetrics();
        MatcherAssert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = Objects.requireNonNull(variables.get("t1"));
        Assert.assertEquals(4, t1.getValues().size());
        assertValue(t1.getValues().get(0), 1d, Unit.SECOND);
        assertValue(t1.getValues().get(1), 0.002d, Unit.SECOND);
        assertValue(t1.getValues().get(2), 0d, Unit.SECOND);
        assertValue(t1.getValues().get(3), 4d, Unit.SECOND);

        MatcherAssert.assertThat(variables, Matchers.hasKey("t2"));
        final Metric t2 = Objects.requireNonNull(variables.get("t2"));
        Assert.assertEquals(3, t2.getValues().size());
        assertValue(t2.getValues().get(0), 5d);
        assertValue(t2.getValues().get(1), 6d);
        assertValue(t2.getValues().get(2), 4d);

        MatcherAssert.assertThat(variables, Matchers.hasKey("g1"));
        final Metric g1 = Objects.requireNonNull(variables.get("g1"));
        Assert.assertEquals(3, g1.getValues().size());
        assertValue(g1.getValues().get(0), 1.2E9d, Unit.BYTE);
        assertValue(g1.getValues().get(1), 1.1d);
        assertValue(g1.getValues().get(2), 0.8d, Unit.SECOND);

        MatcherAssert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = Objects.requireNonNull(variables.get("c1"));
        Assert.assertEquals(1, c1.getValues().size());
        assertValue(c1.getValues().get(0), 1d);
    }

    @Test
    public void testEmpty() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testEmpty.json");
        Assert.assertNotNull(record);

        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertEquals(3, record.getAnnotations().size());
        Assert.assertEquals("MyHost", record.getAnnotations().get("_" + Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get("_" + Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get("_" + Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test(expected = ParsingException.class)
    public void testMissingAnnotationId() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingAnnotationId.json");
    }

    @Test(expected = ParsingException.class)
    public void testEmptyAnnotationId() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testEmptyAnnotationId.json");
    }

    @Test
    public void testNullCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testNullCounters.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testNullTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testNullTimers.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testNullGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testNullGauges.json");
        Assert.assertNotNull(record);
    }

    @Test
    public void testMissingCounters() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testMissingCounters.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testMissingTimers() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testMissingTimers.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testMissingGauges() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testMissingGauges.json");
        Assert.assertNotNull(record);
        Assert.assertTrue(record.getMetrics().isEmpty());
    }

    @Test
    public void testEmptyValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testEmptyValues.json");
        Assert.assertNotNull(record);

        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertEquals(3, record.getAnnotations().size());
        Assert.assertEquals("MyHost", record.getAnnotations().get("_" + Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get("_" + Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get("_" + Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));

        final Map<String, ? extends Metric> variables = record.getMetrics();
        Assert.assertEquals(3, variables.size());

        MatcherAssert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = Objects.requireNonNull(variables.get("t1"));
        Assert.assertTrue(t1.getValues().isEmpty());

        MatcherAssert.assertThat(variables, Matchers.hasKey("g1"));
        final Metric g1 = Objects.requireNonNull(variables.get("g1"));
        Assert.assertTrue(g1.getValues().isEmpty());

        MatcherAssert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = Objects.requireNonNull(variables.get("c1"));
        Assert.assertTrue(c1.getValues().isEmpty());
    }

    @Test
    public void testUpperCaseVersion() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testUpperCaseVersion.json");
        Assert.assertNotNull(record);
    }

    @Test(expected = ParsingException.class)
    public void testBadCounters() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounters.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimers() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimers.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGauges() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGauges.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testNullGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testNullTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testNullCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testNullCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGaugeValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimerValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValues() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounterValues.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingCounterValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingCounterValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingGaugeValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingGaugeValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testMissingTimerValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testMissingTimerValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimerValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGaugeValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValue() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounterValuesValue.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueNumeratorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounterValuesValueNumeratorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueDenominatorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounterValuesValueDenominatorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueNumeratorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGaugeValuesValueNumeratorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueDenominatorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGaugeValuesValueDenominatorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueNumeratorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimerValuesValueNumeratorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueDenominatorUnits() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimerValuesValueDenominatorUnits.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueNumeratorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounterValuesValueNumeratorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadCounterValuesValueDenominatorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadCounterValuesValueDenominatorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueNumeratorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGaugeValuesValueNumeratorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadGaugeValuesValueDenominatorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadGaugeValuesValueDenominatorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueNumeratorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimerValuesValueNumeratorUnitsName.json");
    }

    @Test(expected = ParsingException.class)
    public void testBadTimerValuesValueDenominatorUnitsName() throws ParsingException, IOException {
        parseRecord("QueryLogParserV2fTest/testBadTimerValuesValueDenominatorUnitsName.json");
    }

    @Test
    public void testNaNValues() throws ParsingException, IOException {
        final Record record = parseRecord("QueryLogParserV2fTest/testNaNValues.json");
        Assert.assertNotNull(record);

        Assert.assertEquals(ZonedDateTime.parse("2014-03-24T12:15:41.010Z"), record.getTime());
        Assert.assertEquals(3, record.getAnnotations().size());
        Assert.assertEquals("MyHost", record.getAnnotations().get("_" + Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get("_" + Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get("_" + Key.CLUSTER_DIMENSION_KEY));
        Assert.assertEquals(3, record.getDimensions().size());
        Assert.assertEquals("MyHost", record.getDimensions().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getDimensions().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getDimensions().get(Key.CLUSTER_DIMENSION_KEY));

        final Map<String, ? extends Metric> variables = record.getMetrics();
        Assert.assertEquals(3, variables.size());

        MatcherAssert.assertThat(variables, Matchers.hasKey("t1"));
        final Metric t1 = Objects.requireNonNull(variables.get("t1"));
        Assert.assertTrue(t1.getValues().isEmpty());

        MatcherAssert.assertThat(variables, Matchers.hasKey("g1"));
        final Metric g1 = Objects.requireNonNull(variables.get("g1"));
        Assert.assertTrue(g1.getValues().isEmpty());

        MatcherAssert.assertThat(variables, Matchers.hasKey("c1"));
        final Metric c1 = Objects.requireNonNull(variables.get("c1"));
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
                        JsonToRecordParserV2fTest.class, fileName)));
    }
}
