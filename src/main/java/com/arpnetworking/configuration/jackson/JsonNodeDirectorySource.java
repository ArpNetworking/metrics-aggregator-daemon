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
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * <code>JsonNode</code> based configuration sourced from a directory. This
 * is intended to monitor the files in a single directory and is not designed
 * to monitor a directory tree (e.g. it is not recursive).
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class JsonNodeDirectorySource extends BaseJsonNodeSource {

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
                .put("directory", _directory)
                .put("fileNames", _fileNames)
                .put("fileNamePatterns", _fileNamePatterns)
                .put("jsonNode", _jsonNode)
                .build();
    }

    /* package private */ Optional<JsonNode> getJsonNode() {
        return _jsonNode;
    }

    private boolean isFileMonitored(final File file) {
        if (_fileNames.isEmpty() && _fileNamePatterns.isEmpty()) {
            return true;
        } else {
            if (_fileNames.contains(file.getName())) {
                return true;
            }
            for (final Pattern pattern : _fileNamePatterns) {
                if (pattern.matcher(file.getName()).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    private JsonNodeDirectorySource(final Builder builder) {
        super(builder);
        _directory = builder._directory;
        _fileNames = Sets.newHashSet(builder._fileNames);
        _fileNamePatterns = builder._fileNamePatterns;

        // Build the unified configuration
        final JsonNodeMergingSource.Builder jsonNodeMergingSourceBuilder = new JsonNodeMergingSource.Builder();

        // Process all matching files
        if (_directory.exists() && _directory.isDirectory() && _directory.canRead()) {
            for (final File file : MoreObjects.firstNonNull(_directory.listFiles(), EMPTY_FILE_ARRAY)) {
                if (isFileMonitored(file)) {
                    LOGGER.debug(String.format("Loading configuration file; file=%s", file));
                    jsonNodeMergingSourceBuilder.addSource(new JsonNodeFileSource.Builder()
                            .setFile(file)
                            .build());
                }
            }
        }
        _jsonNode = jsonNodeMergingSourceBuilder.build().getJsonNode();
    }

    private final File _directory;
    private final Set<String> _fileNames;
    private final List<Pattern> _fileNamePatterns;
    private final Optional<JsonNode> _jsonNode;

    private static final File[] EMPTY_FILE_ARRAY = new File[0];
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonNodeDirectorySource.class);

    /**
     * Builder for <code>JsonNodeDirectorySource</code>.
     */
    public static final class Builder extends BaseJsonNodeSource.Builder<Builder, JsonNodeDirectorySource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonNodeDirectorySource::new);
        }

        /**
         * Set the directory.
         *
         * @param value The directory.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDirectory(final File value) {
            _directory = value;
            return this;
        }

        /**
         * Set the <code>Collection</code> of file names. Optional. Default is
         * an empty list (e.g. all files). Cannot be null.
         *
         * <b>Note:</b> Both the file names and file name patterns must be
         * empty (e.g. unset) in order to consider all files in the directory.
         *
         * @param value The <code>Collection</code> of file names.
         * @return This <code>Builder</code> instance.
         */
        public Builder setFileNames(final Collection<String> value) {
            _fileNames = Lists.newArrayList(value);
            return this;
        }

        /**
         * Add a file name.
         *
         * @param value The file name.
         * @return This <code>Builder</code> instance.
         */
        public Builder addFileName(final String value) {
            if (_fileNames == null) {
                _fileNames = Lists.newArrayList(value);
            } else {
                _fileNames.add(value);
            }
            return this;
        }

        /**
         * Set the <code>Collection</code> of file name patterns. Optional.
         * Default is an empty list (e.g. all files). Cannot be null.
         *
         * <b>Note:</b> Both the file names and file name patterns must be
         * empty (e.g. unset) in order to consider all files in the directory.
         *
         * @param value The <code>Collection</code> of file name patterns.
         * @return This <code>Builder</code> instance.
         */
        public Builder setFileNamePatterns(final Collection<Pattern> value) {
            _fileNamePatterns = Lists.newArrayList(value);
            return this;
        }

        /**
         * Add a file name pattern.
         *
         * @param value The file name pattern.
         * @return This <code>Builder</code> instance.
         */
        public Builder addFileNamePattern(final Pattern value) {
            if (_fileNamePatterns == null) {
                _fileNamePatterns = Lists.newArrayList(value);
            } else {
                _fileNamePatterns.add(value);
            }
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
        private File _directory;
        @NotNull
        private List<String> _fileNames = Lists.newArrayList();
        @NotNull
        private List<Pattern> _fileNamePatterns = Lists.newArrayList();
    }
}
