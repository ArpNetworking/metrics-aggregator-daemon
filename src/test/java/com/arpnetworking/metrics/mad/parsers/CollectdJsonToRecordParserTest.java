/**
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

import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for the CollectdJsonToRecord parser.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class CollectdJsonToRecordParserTest {

    @Test
    public void testParse() throws ParsingException, IOException {
        final List<Record> records = parseFile("CollectdJsonParserTest/testParse.json", DEFAULT_HEADERS);

        Assert.assertEquals(18, records.size());
        final Record record = records.get(0);
        Assert.assertEquals("host.example.com", record.getAnnotations().get(Key.HOST_DIMENSION_KEY));
        Assert.assertEquals("MyService", record.getAnnotations().get(Key.SERVICE_DIMENSION_KEY));
        Assert.assertEquals("MyCluster", record.getAnnotations().get(Key.CLUSTER_DIMENSION_KEY));
        verifyMetric(records,
                DateTime.parse("2016-03-31T23:14:46.740Z"),
                "cpu/0/cpu/wait",
                MetricType.COUNTER,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                DateTime.parse("2016-03-31T23:14:46.741Z"),
                "vmem/vmpage_number/file_pages",
                MetricType.GAUGE,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(944937.0)
                                .build()));
        verifyMetric(records,
                DateTime.parse("2016-03-31T23:14:46.741Z"),
                "vmem/vmpage_number/dirty",
                MetricType.GAUGE,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(6463.0)
                                .build()));
        verifyMetric(records,
                DateTime.parse("2016-03-31T23:14:46.741Z"),
                "vmem/vmpage_number/writeback",
                MetricType.GAUGE,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(0.0)
                                .build()));
    }

    private void verifyMetric(final List<Record> records,
            final DateTime timestamp,
            final String name,
            final MetricType type,
            final List<Quantity> values) {
        Assert.assertTrue(records.size() > 0);
        final Record record = records.remove(0);
        Assert.assertEquals(timestamp, record.getTime().withZone(DateTimeZone.UTC));
        final Map<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertTrue(metrics.containsKey(name));
        final Metric metric = metrics.get(name);
        Assert.assertEquals(type, metric.getType());
        for (int i = 0; i < metric.getValues().size(); i++) {
            Assert.assertEquals(values.get(i), metric.getValues().get(i));
        }
    }

    @Test(expected = ParsingException.class)
    public void testParseNoHeaders() throws ParsingException, IOException {
        final ImmutableMultimap<String, String> headers = ImmutableMultimap.<String, String>builder().build();
        parseFile("CollectdJsonParserTest/testParse.json", headers);
    }

    @Test(expected = ParsingException.class)
    public void testParseInvalid() throws ParsingException, IOException {
        parseFile("CollectdJsonParserTest/testParseInvalid.json", DEFAULT_HEADERS);
    }

    private static List<Record> parseFile(final String fileName, final Multimap<String, String> headers)
            throws IOException, ParsingException {
        final byte[] body = Resources.toByteArray(Resources.getResource(CollectdJsonToRecordParser.class, fileName));
        return new CollectdJsonToRecordParser().parse(new HttpRequest(headers, body));
    }

    private static final ImmutableMultimap<String, String> DEFAULT_HEADERS = ImmutableMultimap.<String, String>builder()
            .put("x-tag-service", "MyService")
            .put("x-tag-Cluster", "MyCluster")
            .build();

}
