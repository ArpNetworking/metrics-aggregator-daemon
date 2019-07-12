/*
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

import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Record;
import net.sf.oval.exception.ConstraintsViolatedException;

import java.util.List;

/**
 * Parses the Inscope Metrics protobuf binary protocol in the body of an http request into records.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class ProtobufV2ToRecordParser implements Parser<List<Record>, HttpRequest> {

    private final ProtobufV2bytesToRecordParser _parser = new ProtobufV2bytesToRecordParser();

    @Override
    public List<Record> parse(final HttpRequest data) throws ParsingException {
        try {
            final byte[] bytes = data.getBody().toArray();
            return _parser.parse(bytes);
        } catch (final ConstraintsViolatedException | IllegalArgumentException e) {
            throw new ParsingException("Could not build record", data.getBody().toArray(), e);
        }
    }
}
