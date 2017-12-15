/**
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
package com.arpnetworking.metrics.mad.parsers;

import akka.util.ByteString;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Tests for the V1 protobuf parser.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@SuppressWarnings("deprecation")
public final class ProtobufV1ToRecordParserTest {

    @Test
    public void testParseEmpty() throws ParsingException, IOException {
        final List<Record> records = parseRecords("ProtobufV1ParserTest/testParseEmpty");

        Assert.assertEquals(0, records.size());
    }

    @Test
    public void testParseSingleRecord() throws ParsingException, IOException {
        final UUID uuid = UUID.fromString("142949d2-c0fc-469e-9958-7d2be2c49fa5");
        final DateTime time = new DateTime(1513239602974L);
        final List<Record> records = parseRecords("ProtobufV1ParserTest/testSingleRecord");

        Assert.assertEquals(1, records.size());

        final Record record = records.get(0);

        Assert.assertEquals(uuid, UUID.fromString(record.getId()));
        Assert.assertEquals(time, record.getTime());

        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(2, metrics.size());
        final Metric counter = metrics.get("counter1");
        Assert.assertNotNull(counter);
        Assert.assertEquals(1, counter.getValues().size());
        Assert.assertEquals(new Quantity.Builder().setValue(1d).build(), counter.getValues().get(0));

        final Metric timer = metrics.get("timer1");
        Assert.assertNotNull(timer);
        Assert.assertEquals(1, timer.getValues().size());
        Assert.assertEquals(new Quantity.Builder().setUnit(Unit.MILLISECOND).setValue(508d).build(), timer.getValues().get(0));
    }

    private static List<Record> parseRecords(final String fileName) throws ParsingException, IOException {
        return parseRecords(fileName, createParser());
    }

    private static List<Record> parseRecords(
            final String fileName,
            final Parser<List<Record>, HttpRequest> parser)
            throws ParsingException, IOException {
        final ByteString body =
                ByteString.fromArray(Resources.toByteArray(Resources.getResource(ProtobufV1ToRecordParserTest.class, fileName)));
        return parser.parse(new HttpRequest(ImmutableMultimap.of(), body));
    }

    private static Parser<List<Record>, HttpRequest> createParser() {
        return new ProtobufV1ToRecordParser();
    }
}
