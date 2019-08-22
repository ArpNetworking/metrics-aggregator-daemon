/*
 * Copyright 2019 Dropbox
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

import com.arpnetworking.metrics.mad.parsers.ProtobufV3ToRecordParser;

/**
 * Processes HTTP requests from the metrics client, extracts data and emits metrics.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class ClientHttpSourceV3 extends HttpSource {

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    private ClientHttpSourceV3(final Builder builder) {
        super(builder);
    }

    /**
     * Name of the actor created to receive the HTTP Posts.
     */
    public static final String ACTOR_NAME = "appv3";

    /**
     * ClientHttpSourceV3 {@link BaseSource.Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends HttpSource.Builder<Builder, ClientHttpSourceV3> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(ClientHttpSourceV3::new);
            setActorName(ACTOR_NAME);
            setParser(new ProtobufV3ToRecordParser());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}
