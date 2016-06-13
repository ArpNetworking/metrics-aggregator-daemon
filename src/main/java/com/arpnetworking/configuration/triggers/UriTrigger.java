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
package com.arpnetworking.configuration.triggers;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.configuration.Trigger;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.base.Optional;
import net.sf.oval.constraint.NotNull;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <code>Trigger</code> implementation based on a uri's last modified date and
 * ETag. Either can trigger a reload; the last modified if is later than the
 * previous value or the ETag if it differs from the previous value. If
 * the uri is unavailable it is not considered changed to prevent flickering
 * caused by connectivity or server issues.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class UriTrigger implements Trigger {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean evaluateAndReset() {
        HttpGet request = null;
        try {
            LOGGER.debug()
                    .setMessage("Evaluating trigger")
                    .addData("uri", _uri)
                    .addData("headers", _headers)
                    .log();
            request = new HttpGet(_uri);
            if (!_headers.isEmpty()) {
                request.setHeaders(_headers.toArray(new Header[_headers.size()]));
            }
            if (_previousETag.isPresent()) {
                request.addHeader(HttpHeaders.IF_NONE_MATCH, _previousETag.get());
            }
            if (_previousLastModified.isPresent()) {
                request.addHeader(HttpHeaders.IF_MODIFIED_SINCE, DateUtils.formatDate(_previousLastModified.get()));
            }
            final HttpResponse response = CLIENT.execute(request);
            if (response.getStatusLine().getStatusCode() == 304) {
                LOGGER.debug()
                        .setMessage("Uri unmodified")
                        .addData("uri", _uri)
                        .addData("status", response.getStatusLine().getStatusCode())
                        .log();
                return false;
            }
            if (response.getStatusLine().getStatusCode() / 100 != 2) {
                LOGGER.warn()
                        .setMessage("Failed to retrieve url")
                        .addData("uri", _uri)
                        .addData("status", response.getStatusLine().getStatusCode())
                        .log();
                return false;
            }
            if (response.getFirstHeader(HttpHeaders.ETAG) == null
                    && response.getFirstHeader(HttpHeaders.LAST_MODIFIED) == null) {
                LOGGER.warn()
                        .setMessage("Untriggerable uri missing both etag and last modified")
                        .addData("uri", _uri)
                        .addData("headers", response.getAllHeaders())
                        .log();
                return false;
            }
            return isLastModifiedChanged(response) || isEtagChanged(response);
        } catch (final IOException e) {
            LOGGER.warn()
                    .setMessage("Failed to evaluate url trigger")
                    .addData("uri", _uri)
                    .setThrowable(e)
                    .log();
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
        }

        return false;
    }

    private boolean isEtagChanged(final HttpResponse response) {
        final Header newEtagHeader = response.getFirstHeader(HttpHeaders.ETAG);
        if (newEtagHeader != null) {
            final String newETag = newEtagHeader.getValue();
            if (!_previousETag.isPresent() || !newETag.equals(_previousETag.get())) {
                LOGGER.debug()
                        .setMessage("Uri etag changed")
                        .addData("uri", _uri)
                        .addData("newETag", newETag)
                        .addData("previousETag", _previousETag)
                        .log();
                _previousETag = Optional.of(newETag);
                return true;
            }
        }
        return false;
    }

    private boolean isLastModifiedChanged(final HttpResponse response) {
        final Header newLastModifiedHeader = response.getFirstHeader(HttpHeaders.LAST_MODIFIED);
        if (newLastModifiedHeader != null) {
            final Date newLastModified = DateUtils.parseDate(newLastModifiedHeader.getValue());
            if (newLastModified == null) {
                throw new IllegalArgumentException("Invalid last modified date");
            }
            if (!_previousLastModified.isPresent() || newLastModified.after(_previousLastModified.get())) {
                LOGGER.debug()
                        .setMessage("Uri last modified changed")
                        .addData("uri", _uri)
                        .addData("newLastModified", newLastModified)
                        .addData("previousLastModified", _previousLastModified)
                        .log();
                _previousLastModified = Optional.of(newLastModified);
                return true;
            }
        }
        return false;
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("uri", _uri)
                .put("previousLastModified", _previousLastModified)
                .put("previousETag", _previousETag)
                .put("headers", _headers)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private UriTrigger(final Builder builder) {
        // The uri trigger should always return true on the first successful
        // evaluation while on subsequent evaluations true should only be
        // returned if the content at the uri was changed since the previous
        // evaluation. To accomplish this a modified time of -1 and a null hash
        // is used.
        _uri = builder._uri;
        _previousLastModified = Optional.absent();
        _previousETag = Optional.absent();
        _headers = new ArrayList<>(builder._headers);
    }

    private final URI _uri;

    private Optional<Date> _previousLastModified;
    private Optional<String> _previousETag;
    private List<Header> _headers;

    private static final Logger LOGGER = LoggerFactory.getLogger(UriTrigger.class);
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
     * Builder for <code>UriTrigger</code>.
     */
    public static final class Builder extends OvalBuilder<UriTrigger> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(UriTrigger::new);
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
         * Add a <code>Header</code> to the uri.
         *
         * @param value A HTTP header.
         * @return This <code>Builder</code> instance.
         */
        public Builder addHeader(final Header value) {
            _headers.add(value);
            return this;
        }

        /**
         * Add a <code>List</code> of <code>Header</code> to the uri.
         *
         * @param values A <code>List</code> of HTTP headers.
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

        @NotNull
        private URI _uri;
        @NotNull
        private List<Header> _headers = new ArrayList<>();
    }
}
