/**
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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import net.sf.oval.constraint.NotNull;

import java.io.IOException;

/**
 * <code>JsonNode</code> based configuration sourced from a literal string.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public final class JsonNodeLiteralSource extends BaseJsonNodeSource {

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> getValue(final String... keys) {
        return getValue(getJsonNode(), keys);
    }

    /**
     * {@inheritDoc}
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("source", _source)
                .put("jsonNode", _jsonNode)
                .build();
    }

    /* package private */ Optional<JsonNode> getJsonNode() {
        return _jsonNode;
    }

    private JsonNodeLiteralSource(final Builder builder) {
        super(builder);
        _source = builder._source;

        JsonNode jsonNode = null;
        try {
            jsonNode = _objectMapper.readTree(_source);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
        _jsonNode = Optional.fromNullable(jsonNode);
    }

    private final String _source;
    private final Optional<JsonNode> _jsonNode;

    /**
     * Builder for <code>JsonNodeLiteralSource</code>.
     */
    public static final class Builder extends BaseJsonNodeSource.Builder<Builder, JsonNodeLiteralSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonNodeLiteralSource::new);
        }

        /**
         * Set the source.
         *
         * @param value The source.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSource(final String value) {
            _source = value;
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
        private String _source;
    }
}
