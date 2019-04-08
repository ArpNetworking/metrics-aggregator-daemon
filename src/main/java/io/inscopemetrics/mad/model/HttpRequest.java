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
package io.inscopemetrics.mad.model;

import akka.util.ByteString;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * Represents a parsable HTTP request.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class HttpRequest {
    public Multimap<String, String> getHeaders() {
        return _headers;
    }

    public ByteString getBody() {
        return _body;
    }

    /**
     * Public constructor.
     *
     * @param headers The headers.
     * @param body The body of the request.
     */
    public HttpRequest(final ImmutableMultimap<String, String> headers, final ByteString body) {
        _headers = headers;
        _body = body;
    }

    private final ImmutableMultimap<String, String> _headers;
    private final ByteString _body;
}
