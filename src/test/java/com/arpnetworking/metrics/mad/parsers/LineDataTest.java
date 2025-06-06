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
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Tests for query line parsing.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class LineDataTest {

    @Test
    public void constructTest() {
        final JsonToRecordParser parser = new JsonToRecordParser.Builder().build();
        Assert.assertNotNull(parser);
    }

    @Test(expected = ParsingException.class)
    public void parseUnknownVersion() throws ParsingException {
        final JsonToRecordParser data = new JsonToRecordParser.Builder().build();
        data.parse(UNKNOWN_VERSION_JSON.getBytes(StandardCharsets.UTF_8));
    }

    private static final String UNKNOWN_VERSION_JSON = "{"
            + "  \"version\":\"86x\","
            + "  \"counters\":{"
            + "    \"some_counter\":8"
            + "  },"
            + "  \"timers\":{"
            + "    \"/incentive/bestfor\":[2070]"
            + "  },"
            + "  \"annotations\":{}}";
}
