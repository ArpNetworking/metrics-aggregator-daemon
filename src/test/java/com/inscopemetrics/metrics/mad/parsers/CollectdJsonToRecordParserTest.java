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
package com.inscopemetrics.metrics.mad.parsers;

import akka.util.ByteString;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import com.inscopemetrics.metrics.common.parsers.exceptions.ParsingException;
import com.inscopemetrics.metrics.mad.model.HttpRequest;
import com.inscopemetrics.metrics.mad.model.Metric;
import com.inscopemetrics.metrics.mad.model.Record;
import com.inscopemetrics.tsdcore.model.Key;
import com.inscopemetrics.tsdcore.model.MetricType;
import com.inscopemetrics.tsdcore.model.Quantity;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
                ZonedDateTime.parse("2016-03-31T23:14:46.740Z"),
                "cpu/0/cpu/wait",
                MetricType.COUNTER,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(0.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2016-03-31T23:14:46.741Z"),
                "vmem/vmpage_number/file_pages",
                MetricType.GAUGE,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(944937.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2016-03-31T23:14:46.741Z"),
                "vmem/vmpage_number/dirty",
                MetricType.GAUGE,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(6463.0)
                                .build()));
        verifyMetric(records,
                ZonedDateTime.parse("2016-03-31T23:14:46.741Z"),
                "vmem/vmpage_number/writeback",
                MetricType.GAUGE,
                Collections.singletonList(
                        new Quantity.Builder()
                                .setValue(0.0)
                                .build()));
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
        Assert.assertTrue(metrics.containsKey(name));
        final Metric metric = metrics.get(name);
        Assert.assertEquals(type, metric.getType());
        for (int i = 0; i < metric.getValues().size(); i++) {
            Assert.assertEquals(values.get(i), metric.getValues().get(i));
        }
    }

    @Test(expected = ParsingException.class)
    public void testParseInvalid() throws ParsingException, IOException {
        parseFile("CollectdJsonParserTest/testParseInvalid.json", DEFAULT_HEADERS);
    }

    private static List<Record> parseFile(final String fileName, final ImmutableMultimap<String, String> headers)
            throws IOException, ParsingException {
        final ByteString body =
                ByteString.fromArray(Resources.toByteArray(Resources.getResource(CollectdJsonToRecordParser.class, fileName)));
        return new CollectdJsonToRecordParser().parse(new HttpRequest(headers, body));
    }

    private static final ImmutableMultimap<String, String> DEFAULT_HEADERS = ImmutableMultimap.<String, String>builder()
            .put("x-tag-service", "MyService")
            .put("x-tag-Cluster", "MyCluster")
            .build();
}
