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
package com.arpnetworking.http;

import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Holds an HTTP request and a reply future.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class RequestReply {
    /**
     * Public constructor.
     *
     * @param request the request
     * @param response the reply
     */
    public RequestReply(final HttpRequest request, final CompletableFuture<HttpResponse> response) {
        _request = request;
        _response = response;
    }

    public HttpRequest getRequest() {
        return _request;
    }

    public CompletableFuture<HttpResponse> getResponse() {
        return _response;
    }

    private final HttpRequest _request;
    private final CompletableFuture<HttpResponse> _response;
}
