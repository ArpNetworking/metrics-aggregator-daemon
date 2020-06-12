/*
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.configuration.jackson;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;

import java.util.List;

/**
 * Static configuration implementation of {@link com.arpnetworking.configuration.Configuration}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class StaticConfiguration extends BaseJacksonConfiguration {

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("source", _source)
                .build();
    }

    @Override
    protected JsonNodeSource getJsonSource() {
        return _source;
    }

    private StaticConfiguration(final Builder builder) {
        super(builder);
        _source = new JsonNodeMergingSource.Builder()
            .setSources(builder._sources)
            .build();
    }

    private final JsonNodeSource _source;

    /**
     * Builder for {@link StaticConfiguration}.
     */
    public static final class Builder extends BaseJacksonConfiguration.Builder<Builder, StaticConfiguration> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(StaticConfiguration::new);
        }

        /**
         * Set the {@link List} of {@link JsonNodeSource}
         * instances. Cannot be null.
         *
         * @param value The {@link List} of {@link JsonNodeSource} instances.
         * @return This {@link Builder} instance.
         */
        public Builder setSources(final List<JsonNodeSource> value) {
            _sources = Lists.newArrayList(value);
            return self();
        }

        /**
         * Add a {@link JsonNodeSource} instance.
         *
         * @param value The {@link JsonNodeSource} instance.
         * @return This {@link Builder} instance.
         */
        public Builder addSource(final JsonNodeSource value) {
            if (_sources == null) {
                _sources = Lists.newArrayList(value);
            } else {
                _sources.add(value);
            }
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private List<JsonNodeSource> _sources;
    }
}
