/*
 * Copyright 2018 Bruno Green.
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
import net.sf.oval.constraint.NotNull;

/**
 * Processes Prometheus messages, extracts data and emits metrics.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
public final class PrometheusHttpSource extends HttpSource{
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
        }

        /**
         * Whether to interpret units in the metric name. Optional. Defaults to false. Cannot be null.
         *
         * @param value the value
         * @return this {@link Builder}
         */
        public Builder setInterpretUnits(final Boolean value) {
            _interpretUnits = value;
            return this;
        }

        /**
         * Whether to output debug files with the raw prometheus data. Cannot be null.
         *
         * @param value the value
         * @return this {@link Builder}
         */
        public Builder setOutputDebugFiles(final Boolean value) {
            _outputDebugFiles = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Boolean _interpretUnits = false;

        @NotNull
        private Boolean _outputDebugFiles = false;

        @Override
        public PrometheusHttpSource build() {
            setParser(new PrometheusToRecordParser(_interpretUnits, _outputDebugFiles));
            return super.build();
        }
    }

}
