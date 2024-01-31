/*
 * Copyright 2016 Smartsheet
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

import org.apache.pekko.util.ByteString;
import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Tests for the {@link CollectdJsonToRecordParser} parser.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class CollectdJsonToRecordParserTest {

    private static final ImmutableMultimap<String, String> DEFAULT_HEADERS = ImmutableMultimap.<String, String>builder()
            .put("x-tag-service", "MyService")
            .put("x-tag-Cluster", "MyCluster")
            .build();

    private final Supplier<CollectdJsonToRecordParser.CollectdRecord.Builder> _collectdRecordBuilder =
            () -> new CollectdJsonToRecordParser.CollectdRecord.Builder()
                    .setDsNames(ImmutableList.of("value"))
                    .setDsTypes(ImmutableList.of("counter"))
                    .setHost("localhost")
                    .setPlugin("cpu")
                    .setPluginInstance("0")
                    .setTime((double) System.currentTimeMillis())
                    .setType("cpu")
                    .setTypeInstance("idle")
                    .setValues(ImmutableList.of(11d));


    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _collectdRecordBuilder.get(),
                CollectdJsonToRecordParser.CollectdRecord.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_collectdRecordBuilder.get());
    }

    @Test
    public void testToString() {
        final String asString = _collectdRecordBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testParse() throws ParsingException, IOException {
        final List<Record> records = parseFile("CollectdJsonParserTest/testParse.json", DEFAULT_HEADERS);

        Assert.assertEquals(18, records.size());
        final Record record = records.get(0);
        Assert.assertEquals("host.example.com", record.getAnnotations().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get(Key.CLUSTER_DIMENSION_KEY));
        verifyMetric(records,
                ZonedDateTime.parse("2016-03-31T23:14:46.740Z"),
                "cpu/0/cpu/wait",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
    }

    @Test(expected = ParsingException.class)
    public void testParseInvalid() throws ParsingException, IOException {
        parseFile("CollectdJsonParserTest/testParseInvalid.json", DEFAULT_HEADERS);
    }

    private void verifyMetric(final List<Record> records,
                              final ZonedDateTime timestamp,
                              final String name,
                              final MetricType type,
                              final List<Quantity> values) {
        Assert.assertTrue(!records.isEmpty());
        final Record record = records.remove(0);
        Assert.assertEquals(timestamp, record.getTime().withZoneSameInstant(ZoneOffset.UTC));
        final Map<String, ? extends Metric> metrics = record.getMetrics();
        MatcherAssert.assertThat(metrics, Matchers.hasKey(name));
        final Metric metric = metrics.get(name);
        Assert.assertEquals(type, metric.getType());
        for (int i = 0; i < metric.getValues().size(); i++) {
            Assert.assertEquals(values.get(i), metric.getValues().get(i));
        }
    }

    private static List<Record> parseFile(final String fileName, final ImmutableMultimap<String, String> headers)
            throws IOException, ParsingException {
        final ByteString body =
                ByteString.fromArray(Resources.toByteArray(Resources.getResource(CollectdJsonToRecordParser.class, fileName)));
        return new CollectdJsonToRecordParser().parse(new HttpRequest(headers, body));
    }

    // CHECKSTYLE.OFF: MethodLengthCheck - This is a long test, many assertions
    @Test
    public void testParseNullValues() throws ParsingException, IOException {
        final List<Record> records = parseFile("CollectdJsonParserTest/testParseNullValues.json", DEFAULT_HEADERS);

        Assert.assertEquals(15, records.size());
        final Record record = records.get(0);
        Assert.assertEquals("ip-10-1-19-254.us-east-2.compute.internal", record.getAnnotations().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get(Key.CLUSTER_DIMENSION_KEY));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "tcpconns/all/tcp_connections/CLOSING",
                MetricType.GAUGE,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "conntrack/conntrack",
                MetricType.GAUGE,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(3019.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "conntrack/conntrack/max",
                MetricType.GAUGE,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(131072.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/RtoMin",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/RtoMax",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/MaxConn",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/ActiveOpens",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(29.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/PassiveOpens",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(2.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/AttemptFails",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/EstabResets",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/InSegs",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(464.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/OutSegs",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(462.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/RetransSegs",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/InErrs",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2022-08-17T00:24:51.300Z"),
                "protocols/Tcp/protocol_counter/OutRsts",
                MetricType.COUNTER,
                Collections.singletonList(
                        new DefaultQuantity.Builder()
                                .setValue(2.0)
                                .build()));
    }
    // CHECKSTYLE:ON MethodLength
}
