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
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.io.Resources;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests for the CollectdJsonToRecord parser.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class CollectdJsonToRecordParserTest {
    @Test
    public void testParse() throws ParsingException, IOException {
        final List<DefaultRecord.Builder> builders = parseFile("CollectdJsonParserTest/testParse.json");

        final List<Record> records = builders.stream()
                .map(DefaultRecord.Builder::build)
                .collect(Collectors.toList());

        Assert.assertEquals(18, records.size());
        final Record record = records.get(0);
        final Map<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(DateTime.parse("2016-03-31T23:14:46.740Z"), record.getTime().toDateTime(DateTimeZone.UTC));
        final Metric metric = new DefaultMetric.Builder()
                .setType(MetricType.COUNTER)
                .setValues(
                        Collections.singletonList(
                                new Quantity.Builder()
                                        .setValue(0D)
                                        .build()))
                .build();
        Assert.assertEquals(metric, metrics.get("cpu/0/cpu/wait/value"));
    }

    @Test(expected = ParsingException.class)
    public void testParseInvalid() throws ParsingException, IOException {
        parseFile("CollectdJsonParserTest/testParseInvalid.json");
    }

    private static List<DefaultRecord.Builder> parseFile(final String fileName) throws IOException, ParsingException {
        final List<DefaultRecord.Builder> builders = new CollectdJsonToRecordParser()
                .parse(Resources.toByteArray(Resources.getResource(CollectdJsonToRecordParser.class, fileName)));
        for (final DefaultRecord.Builder builder : builders) {
            builder.setService("MyService");
            builder.setCluster("MyCluster");
        }
        return builders;
    }
}
