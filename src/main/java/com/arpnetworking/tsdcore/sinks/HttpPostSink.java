/*
 * Copyright 2014 Brandon Arp
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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.MediaTypes;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.uri.Uri;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;

/**
 * Publishes to an HTTP endpoint. This class is thread safe.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public abstract class HttpPostSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData data) {
        _sinkActor.tell(new HttpPostSinkActor.EmitAggregation(data), ActorRef.noSender());
    }

    @Override
    public void close() {
        LOGGER.info()
                .setMessage("Closing sink")
                .addData("sink", getName())
                .addData("uri", _uri)
                .log();
        _sinkActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("actor", _sinkActor)
                .put("uri", _uri)
                .build();
    }

    /**
     * Creates an HTTP request from a serialized data entry. Default is an <code>HttpPost</code> containing
     * serializedData as the body with content type of application/json
     *
     * @param client The http client to build the request with.
     * @param serializedData The serialized data.
     * @return <code>HttpRequest</code> to execute
     */
    protected Request createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        return new RequestBuilder()
                .setUri(_aysncHttpClientUri)
                .setHeader("Content-Type", getContentType())
                .setBody(serializedData)
                .setMethod(HttpMethods.POST.value())
                .build();
    }

    /**
     * Return the media type for this sink's http post payload.
     *
     * @return the media type for this sink's http post payload
     */
    protected String getContentType() {
        return MediaTypes.APPLICATION_JSON.toString();
    }

    /**
     * Create HTTP requests for each serialized data entry. The list is
     * guaranteed to be non-empty.
     *
     * @param client The http client to build the request with.
     * @param periodicData The <code>PeriodicData</code> to be serialized.
     * @return The <code>HttpRequest</code> instance to execute.
     */
    protected Collection<Request> createRequests(
            final AsyncHttpClient client,
            final PeriodicData periodicData) {
        final Collection<byte[]> serializedData = serialize(periodicData);
        final Collection<Request> requests = Lists.newArrayListWithExpectedSize(serializedData.size());
        for (final byte[] serializedDatum : serializedData) {
            requests.add(createRequest(client, serializedDatum));
        }
        return requests;
    }

    /**
     * Accessor for the <code>URI</code>.
     *
     * @return The <code>URI</code>.
     */
    protected URI getUri() {
        return _uri;
    }

    /**
     * Accessor for the AysncHttpClient <code>Uri</code>.
     *
     * @return The AysncHttpClient <code>Uri</code>.
     */
    protected Uri getAysncHttpClientUri() {
        return _aysncHttpClientUri;
    }

    /**
     * Serialize the <code>PeriodicData</code> and <code>Condition</code> instances
     * for posting.
     *
     * @param periodicData The <code>PeriodicData</code> to be serialized.
     * @return The serialized representation of <code>PeriodicData</code>.
     */
    protected abstract Collection<byte[]> serialize(PeriodicData periodicData);

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected HttpPostSink(final Builder<?, ?> builder) {
        super(builder);
        _uri = builder._uri;
        _aysncHttpClientUri = Uri.create(_uri.toString());

        _sinkActor = builder._actorSystem.actorOf(
                HttpPostSinkActor.props(CLIENT, this, builder._maximumConcurrency, builder._maximumQueueSize,
                        builder._spreadPeriod, builder._metricsFactory));
    }

    private final URI _uri;
    private final Uri _aysncHttpClientUri;
    private final ActorRef _sinkActor;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpPostSink.class);
    private static final AsyncHttpClient CLIENT;

    static {
        final DefaultAsyncHttpClientConfig.Builder clientConfigBuilder = new DefaultAsyncHttpClientConfig.Builder();
        clientConfigBuilder.setThreadPoolName("HttpPostSinkWorker");
        final AsyncHttpClientConfig clientConfig = clientConfigBuilder.build();
        CLIENT = new DefaultAsyncHttpClient(clientConfig);
    }

    /**
     * Implementation of abstract builder pattern for <code>HttpPostSink</code>.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public abstract static class Builder<B extends BaseSink.Builder<B, S>, S extends HttpPostSink> extends BaseSink.Builder<B, S> {

        /**
         * The <code>URI</code> to post the aggregated data to. Cannot be null.
         *
         * @param value The <code>URI</code> to post the aggregated data to.
         * @return This instance of <code>Builder</code>.
         */
        public B setUri(final URI value) {
            _uri = value;
            return self();
        }

        /**
         * Sets the actor system to create the sink actor in. Required. Cannot be null. Injected by default.
         *
         * @param value the actor system
         * @return this builder
         */
        public B setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return self();
        }

        /**
         * Sets the maximum concurrency of the http requests. Optional. Cannot be null.
         * Default is 1. Minimum is 1.
         *
         * @param value the maximum concurrency
         * @return this builder
         */
        public B setMaximumConcurrency(final Integer value) {
            _maximumConcurrency = value;
            return self();
        }

        /**
         * Sets the maximum delay before starting to send data to the server. Optional. Cannot be null.
         * Default is 0.
         *
         * @param value the maximum delay before sending new data
         * @return this builder
         */
        public B setSpreadPeriod(final Duration value) {
            _spreadPeriod = value;
            return self();
        }

        /**
         * Sets the maximum pending queue size. Optional Cannot be null.
         * Default is 25000. Minimum is 1.
         *
         * @param value the maximum pending queue size
         * @return this builder
         */
        public B setMaximumQueueSize(final Integer value) {
            _maximumQueueSize = value;
            return self();
        }

        /**
         * Instance of <code>MetricsFactory</code>. Cannot be null. This field
         * may be injected automatically by Jackson/Guice if setup to do so.
         *
         * @param value Instance of <code>MetricsFactory</code>.
         * @return This instance of <code>Builder</code>.
         */
        public B setMetricsFactory(final MetricsFactory value) {
            _metricsFactory = value;
            return self();
        }

        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor The constructor for the concrete type to be created by this builder.
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        @NotNull
        private URI _uri;
        @JacksonInject
        @NotNull
        private ActorSystem _actorSystem;
        @NotNull
        @Min(1)
        private Integer _maximumConcurrency = 1;
        @NotNull
        @Min(1)
        private Integer _maximumQueueSize = 25000;
        @NotNull
        private Duration _spreadPeriod = Duration.ZERO;
        @JacksonInject
        @NotNull
        private MetricsFactory _metricsFactory;
    }
}
