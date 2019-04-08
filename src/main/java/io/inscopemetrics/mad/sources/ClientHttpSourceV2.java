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
package io.inscopemetrics.mad.sources;

import io.inscopemetrics.mad.parsers.ProtobufV2ToRecordParser;

/**
 * Processes HTTP requests from the metrics client, extracts data and emits metrics.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class ClientHttpSourceV2 extends HttpSource {

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    private ClientHttpSourceV2(final Builder builder) {
        super(builder);
    }

    /**
     * Name of the actor created to receive the HTTP Posts.
     */
    public static final String ACTOR_NAME = "appv2";

    /**
     * ClientHttpSourceV2 {@link BaseSource.Builder} implementation.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
     */
    public static final class Builder extends HttpSource.Builder<Builder, ClientHttpSourceV2> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(ClientHttpSourceV2::new);
            setActorName(ACTOR_NAME);
            setParser(new ProtobufV2ToRecordParser());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}
