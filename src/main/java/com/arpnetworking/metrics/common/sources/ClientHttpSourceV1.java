/**
 * Copyright 2016 Inscope Metrics, Inc.
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
package com.arpnetworking.metrics.common.sources;

import com.arpnetworking.metrics.mad.parsers.ProtobufToRecordParser;

/**
 * Processes HTTP requests from the metrics client, extracts data and emits metrics.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class ClientHttpSourceV1 extends HttpSource {

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    private ClientHttpSourceV1(final Builder builder) {
        super(builder);
    }

    /**
     * Name of the actor created to receive the HTTP Posts.
     */
    public static final String ACTOR_NAME = "appv1";

    /**
     * ClientHttpSourceV1 {@link BaseSource.Builder} implementation.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder extends HttpSource.Builder<Builder, ClientHttpSourceV1> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(ClientHttpSourceV1::new);
            setActorName(ACTOR_NAME);
            setParser(new ProtobufToRecordParser());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}
