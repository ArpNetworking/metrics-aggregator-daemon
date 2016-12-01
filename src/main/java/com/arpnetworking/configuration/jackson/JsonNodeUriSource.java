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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;
import net.sf.oval.constraint.NotNull;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * <code>JsonNode</code> based configuration sourced from a file.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class JsonNodeUriSource extends BaseJsonNodeSource {

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
                .put("jsonNode", _jsonNode)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        return toLogValue().toString();
    }

    /* package private */ Optional<JsonNode> getJsonNode() {
        return _jsonNode;
    }

    private JsonNodeUriSource(final Builder builder) {
        super(builder);
        _uri = builder._uri;
        final List<Header> headers = new ArrayList<>(builder._headers);

        JsonNode jsonNode = null;
        HttpGet request = null;
        try {
            request = new HttpGet(_uri);
            if (!headers.isEmpty()) {
                //NOTE: This overrides all headers for this request. If other headers are to be added, use addHeader
                //method after this or change this to addHeaders
                request.setHeaders(headers.toArray(new Header[headers.size()]));
            }
            final HttpResponse response = CLIENT.execute(request);
            jsonNode = _objectMapper.readTree(response.getEntity().getContent());
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
        }
        _jsonNode = Optional.ofNullable(jsonNode);
    }

    private final URI _uri;
    private final Optional<JsonNode> _jsonNode;

    private static final int CONNECTION_TIMEOUT_IN_MILLISECONDS = 3000;
    private static final HttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    private static final HttpClient CLIENT = HttpClientBuilder.create()
            .setConnectionManager(CONNECTION_MANAGER)
            .setDefaultRequestConfig(
                    RequestConfig.copy(RequestConfig.DEFAULT)
                            .setConnectTimeout(CONNECTION_TIMEOUT_IN_MILLISECONDS)
                            .build())
            .build();

    /**
     * Builder for <code>JsonNodeUriSource</code>.
     */
    public static final class Builder extends BaseJsonNodeSource.Builder<Builder, JsonNodeUriSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonNodeUriSource::new);
        }

        /**
         * Set the source <code>URI</code>.
         *
         * @param value The source <code>URI</code>.
         * @return This <code>Builder</code> instance.
         */
        public Builder setUri(final URI value) {
            _uri = value;
            return this;
        }

        /**
         * Add a request header for the source.
         *
         * @param value The <code>Header</code> value.
         * @return This <code>Builder</code> instance.
         */
        public Builder addHeader(final Header value) {
            _headers.add(value);
            return this;
        }

        /**
         * Add a list of request headers for the source.
         *
         * @param values The <code>List</code> of <code>Header</code> values.
         * @return This <code>Builder</code> instance.
         */
        public Builder addHeaders(final List<Header> values) {
            _headers.addAll(values);
            return this;
        }

        /**
         * Overrides the existing headers with  a <code>List</code> of <code>Header</code>.
         *
         * @param values A <code>List</code> of HTTP headers.
         * @return This <code>Builder</code> instance.
         */
        public Builder setHeaders(final List<Header> values) {
            _headers = new ArrayList<>(values);
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
        @NotNull
        private List<Header> _headers = new ArrayList<>();
    }
}
