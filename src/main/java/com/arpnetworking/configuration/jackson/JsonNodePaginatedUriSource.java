/**
 * Copyright 2015 Groupon.com
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
import com.google.common.base.Throwables;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

/**
 * <code>JsonNode</code> based configuration sourced from a paginated <code>URI</code>.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class JsonNodePaginatedUriSource extends BaseJsonNodeSource {

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
                .put("uri", _uri)
                .put("dataKeys", _dataKeys)
                .put("nextPageKeys", _nextPageKeys)
                .put("mergingSource", _mergingSource)
                .build();
    }


    /* package private */ Optional<JsonNode> getJsonNode() {
        return _mergingSource.getValue(_dataKeys);
    }

    private JsonNodePaginatedUriSource(final Builder builder) {
        super(builder);
        _uri = builder._uri;
        _dataKeys = builder._dataKeys.toArray(new String[builder._dataKeys.size()]);
        _nextPageKeys = builder._nextPageKeys.toArray(new String[builder._nextPageKeys.size()]);

        final JsonNodeMergingSource.Builder mergingSourceBuilder = new JsonNodeMergingSource.Builder();
        try {
            final URIBuilder uriBuilder = new URIBuilder(_uri);
            URI currentUri = uriBuilder.build();
            while (currentUri != null) {
                LOGGER.debug()
                        .setMessage("Creating JsonNodeUriSource for page")
                        .addData("uri", currentUri)
                        .log();

                // Create a URI source for the page
                final JsonNodeUriSource uriSource = new JsonNodeUriSource.Builder()
                        .setUri(currentUri)
                        .build();
                mergingSourceBuilder.addSource(uriSource);

                // Extract the link for the next page
                final Optional<JsonNode> nextPageNode = uriSource.getValue(_nextPageKeys);
                if (nextPageNode.isPresent() && !nextPageNode.get().isNull()) {
                    final String nextPagePath = nextPageNode.get().asText();

                    final URI nextPageUri = URI.create(nextPagePath.startsWith("/") ? nextPagePath : "/" + nextPagePath);
                    final URIBuilder nextPageUriBuilder = new URIBuilder(nextPageUri);
                    currentUri = uriBuilder
                            .setPath(nextPageUri.getPath())
                            .setParameters(nextPageUriBuilder.getQueryParams())
                            .build();
                } else {
                    currentUri = null;
                }
            }
        } catch (final URISyntaxException e) {
            throw Throwables.propagate(e);
        }

        _mergingSource = mergingSourceBuilder.build();
    }

    private final URI _uri;
    private final String[] _dataKeys;
    private final String[] _nextPageKeys;
    private final JsonNodeMergingSource _mergingSource;

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonNodePaginatedUriSource.class);

    /**
     * Builder for <code>JsonNodeUrlSource</code>.
     */
    public static final class Builder extends BaseJsonNodeSource.Builder<Builder, JsonNodePaginatedUriSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonNodePaginatedUriSource::new);
        }

        /**
         * Set the source <code>URI</code>. Required. The full URI to
         * the first page's results. The protocol, host and port will
         * be used from this uri to complete the path-only uri found
         * at each next page key.
         *
         * @param value The source <code>URL</code>.
         * @return This <code>Builder</code> instance.
         */
        public Builder setUri(final URI value) {
            _uri = value;
            return this;
        }

        /**
         * Set the keys to the data. Required. Cannot be null
         * or empty. The value at the end of this key chain should
         * be an array.
         *
         * @param value The keys from the root to the data.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDataKeys(final List<String> value) {
            _dataKeys = value;
            return this;
        }

        /**
         * Set the keys to the next page uri. Required. Cannot
         * be null or empty. The value at this key should be a path-only
         * URL (e.g. without protocol, host or port) to the next
         * page's results.
         *
         * @param value The keys from the root to the next page uri.
         * @return This <code>Builder</code> instance.
         */
        public Builder setNextPageKeys(final List<String> value) {
            _nextPageKeys = value;
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
        private URI _uri;
        @NotEmpty
        @NotNull
        private List<String> _dataKeys;
        @NotEmpty
        @NotNull
        private List<String> _nextPageKeys;
    }
}
