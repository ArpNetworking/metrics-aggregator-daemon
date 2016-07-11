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
package com.arpnetworking.metrics.mad.model;

import com.google.common.collect.Multimap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Represents a parsable HTTP request.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public final class HttpRequest {
    public Multimap<String, String> getHeaders() {
        return _headers;
    }

    public byte[] getBody() {
        return _body;
    }

    /**
     * Public constructor.
     *
     * @param headers The headers.
     * @param body The body of the request.
     */
    public HttpRequest(final Multimap<String, String> headers, final byte[] body) {
        _headers = headers;
        _body = body;
    }

    private final Multimap<String, String> _headers;
    private final byte[] _body;
}
