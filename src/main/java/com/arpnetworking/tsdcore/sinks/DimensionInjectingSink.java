/*
 * Copyright 2016 Groupon.com
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.util.Map;

/**
 * Sink adds any specified dimensions.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class DimensionInjectingSink extends BaseSink {

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData data) {
        final Map<String, String> mergedDimensions = Maps.newHashMap(_defaultDimensions);
        mergedDimensions.putAll(data.getDimensions().getParameters());
        mergedDimensions.putAll(_overrideDimensions);
        final PeriodicData.Builder dataBuilder = PeriodicData.Builder.clone(data);
        dataBuilder.setDimensions(new DefaultKey(ImmutableMap.copyOf(mergedDimensions)));
        _sink.recordAggregateData(dataBuilder.build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // Nothing to do
    }

    private DimensionInjectingSink(final Builder builder) {
        super(builder);
        _sink = builder._sink;
        _defaultDimensions = builder._defaultDimensions;
        _overrideDimensions = builder._overrideDimensions;
    }

    private final Sink _sink;
    private final ImmutableMap<String, String> _defaultDimensions;
    private final ImmutableMap<String, String> _overrideDimensions;

    /**
     * Implementation of builder pattern for {@link DimensionInjectingSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends BaseSink.Builder<Builder, DimensionInjectingSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DimensionInjectingSink::new);
        }

        /**
         * Sets the default dimensions to inject. Matching dimensions in the data
         * overwrite any default dimensions.
         *
         * @param value The default dimensions to inject.
         * @return This instance of {@link Builder}.
         */
        public Builder setDefaultDimensions(final ImmutableMap<String, String> value) {
            _defaultDimensions = value;
            return self();
        }

        /**
         * Sets the override dimensions to inject. Matching dimensions in the data
         * are overwritten by override dimensions.
         *
         * @param value The overrride dimensions to inject.
         * @return This instance of {@link Builder}.
         */
        public Builder setOverrideDimensions(final ImmutableMap<String, String> value) {
            _overrideDimensions = value;
            return self();
        }

        /**
         * The sink to wrap. Cannot be null.
         *
         * @param value The sink to wrap.
         * @return This instance of {@link Builder}.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private ImmutableMap<String, String> _defaultDimensions = ImmutableMap.of();
        @NotNull
        private ImmutableMap<String, String> _overrideDimensions = ImmutableMap.of();
        @NotNull
        private Sink _sink;
    }
}
