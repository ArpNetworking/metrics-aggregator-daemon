/*
 * Copyright 2020 Dropbox
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import net.sf.oval.constraint.NotNull;
import org.asynchttpclient.Request;

import java.time.Instant;

/**
 * Contains the info for a http request.
 *
 * @author Qinyan Li (lqy520s at hotmail dot com)
 */
public final class RequestEntry {
    public Request getRequest() {
        return _request;
    }

    public Instant getEnterTime() {
        return _enterTime;
    }

    public long getPopulationSize() {
        return _populationSize;
    }

    private RequestEntry(final Builder builder) {
        _request = builder._request;
        _enterTime = builder._enterTime;
        _populationSize = builder._populationSize;
    }

    private final Request _request;
    private Instant _enterTime;
    private final long _populationSize;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link RequestEntry}.
     *
     * TODO(ville): Convert RequestEntry.Builder would be a ThreadLocalBuilder
     * See comments in HttpPostSink:createRequests
     */
    public static final class Builder extends OvalBuilder<RequestEntry> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(RequestEntry::new);
        }

        /**
         * Set the request. Required. Cannot be null.
         *
         * @param value The request.
         * @return This {@link Builder} instance.
         */
        public Builder setRequest(final Request value) {
            _request = value;
            return this;
        }

        /**
         * Set the time when the request enter the pending request queue. Required. Cannot be null.
         *
         * @param value The enter time.
         * @return This {@link Builder} instance.
         */
        public Builder setEnterTime(final Instant value) {
            _enterTime = value;
            return this;
        }

        /**
         * Set the population size of the request. Required. Cannot be null.
         *
         * @param value The population size.
         * @return This {@link Builder} instance.
         */
        public Builder setPopulationSize(final long value) {
            _populationSize = value;
            return this;
        }

        @NotNull
        private Request _request;
        @NotNull
        private Instant _enterTime;
        @NotNull
        private Long _populationSize;
    }
}
