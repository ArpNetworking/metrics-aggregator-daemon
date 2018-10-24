/*
 * Copyright 2018 Inscope Metrics, Inc.
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

import com.arpnetworking.metrics.mad.parsers.PrometheusToRecordParser;

/**
 * Processes Prometheus messages, extracts data and emits metrics.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
public final class PrometheusHttpSource extends HttpSource{
    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    private PrometheusHttpSource(final HttpSource.Builder<?, ? extends HttpSource> builder) {
        super(builder);
    }

    /**
     * Name of the actor created to receive the HTTP Posts.
     */
    public static final String ACTOR_NAME = "prometheus";

    /**
     * PrometheusHttpSource {@link BaseSource.Builder} implementation.
     */
    public static final class Builder extends HttpSource.Builder<Builder, PrometheusHttpSource> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(PrometheusHttpSource::new);
            setActorName(ACTOR_NAME);
            setParser(new PrometheusToRecordParser());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}
