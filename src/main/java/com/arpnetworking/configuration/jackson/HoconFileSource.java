/**
 * Copyright 2016 Inscope Metrics Inc.
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
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import net.sf.oval.constraint.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Typesafe config based configuration sourced from a file and mapped to a <code>JsonNode</code>.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class HoconFileSource extends BaseJsonNodeSource {

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
                .put("file", _file)
                .put("jsonNode", _jsonNode)
                .build();
    }

    /* package private */ Optional<JsonNode> getJsonNode() {
        return _jsonNode;
    }

    private HoconFileSource(final Builder builder) {
        super(builder);
        _file = builder._file;

        JsonNode jsonNode = null;
        if (_file.canRead()) {
            try {
                final Config config = ConfigFactory.parseFile(_file);
                final String hoconAsJson = config.resolve().root().render(ConfigRenderOptions.concise());
                jsonNode = _objectMapper.readTree(hoconAsJson);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else if (builder._file.exists()) {
            LOGGER.warn()
                    .setMessage("Cannot read file")
                    .addData("file", _file)
                    .log();
        } else {
            LOGGER.debug()
                    .setMessage("File does not exist")
                    .addData("file", _file)
                    .log();
        }
        _jsonNode = Optional.ofNullable(jsonNode);
    }

    private final File _file;
    private final Optional<JsonNode> _jsonNode;

    private static final Logger LOGGER = LoggerFactory.getLogger(HoconFileSource.class);

    /**
     * Builder for <code>HoconFileSource</code>.
     */
    public static final class Builder extends BaseJsonNodeSource.Builder<Builder, HoconFileSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(HoconFileSource::new);
        }

        /**
         * Set the source <code>File</code>.
         *
         * @param value The source <code>File</code>.
         * @return This <code>Builder</code> instance.
         */
        public Builder setFile(final File value) {
            _file = value;
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
        private File _file;
    }
}
