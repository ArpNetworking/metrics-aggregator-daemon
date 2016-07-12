/**
 * Copyright 2016 Smartsheet
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

import akka.Done;
import akka.NotUsed;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpResponse;
import akka.http.scaladsl.model.HttpRequest;
import akka.http.scaladsl.model.RequestEntity;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.FanInShape2;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.Supervision;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Zip;
import akka.util.ByteString;
import com.arpnetworking.http.RequestReply;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.parsers.CollectdJsonToRecordParser;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Processes HTTP posts from Collectd, extracts data and emits metrics.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class CollectdHttpSourceV1 extends ActorSource {
    /**
     * {@inheritDoc}
     */
    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected CollectdHttpSourceV1(final Builder builder) {
        super(builder);
        _parser = builder._parser;
    }

    private final Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser;

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends UntypedActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link CollectdHttpSourceV1} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final CollectdHttpSourceV1 source) {
            return Props.create(Actor.class, source);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onReceive(final Object message) throws Exception {
            if (message instanceof RequestReply) {
                final RequestReply requestReply = (RequestReply) message;
                // TODO(barp): Fix the ugly HttpRequest cast here due to java vs scala dsl
                Source.single((HttpRequest) requestReply.getRequest())
                        .via(_processGraph)
                        .toMat(_sink, Keep.right())
                        .run(_materializer)
                        .whenComplete((done, err) -> {
                            final CompletableFuture<HttpResponse> responseFuture = requestReply.getResponse();
                            if (err == null) {
                                responseFuture.complete(HttpResponse.create().withStatus(200));
                            } else {
                                BAD_REQUEST_LOGGER.warn()
                                        .setMessage("Error handling collectd post")
                                        .setThrowable(err)
                                        .log();
                                if (err instanceof ParsingException) {
                                    responseFuture.complete(HttpResponse.create().withStatus(400));
                                } else {
                                    responseFuture.complete(HttpResponse.create().withStatus(500));
                                }
                            }
                        });
            } else {
                unhandled(message);
            }
        }

        /**
         * Constructor.
         *
         * @param source The {@link CollectdHttpSourceV1} to send notifications through.
         */
        /* package private */ Actor(final CollectdHttpSourceV1 source) {
            _parser = source._parser;
            _sink = Sink.foreach(source::notify);
            _materializer = ActorMaterializer.create(
                    ActorMaterializerSettings.create(context().system())
                            .withSupervisionStrategy(Supervision.stoppingDecider()),
                    context());

            _processGraph = GraphDSL.create(builder -> {

                // Flows
                final Flow<HttpRequest, byte[], NotUsed> getBodyFlow = Flow.<HttpRequest>create()
                        .map(HttpRequest::entity)
                        .flatMapConcat(RequestEntity::getDataBytes)
                        .map(ByteString::toArray) // Transform to array form
                        .named("getBody");

                final Flow<HttpRequest, Multimap<String, String>, NotUsed> getHeadersFlow = Flow.<HttpRequest>create()
                        .map(HttpRequest::getHeaders)
                        .map(Actor::createHeaderMultimap) // Transform to array form
                        .named("getHeaders");

                final Flow<Pair<byte[], Multimap<String, String>>, Record, NotUsed> createAndParseFlow =
                        Flow.<Pair<byte[], Multimap<String, String>>>create()
                                .map(Actor::mapModel)
                                .mapConcat(this::parseRecords) // Parse the json string into a record builder
                                // NOTE: this should be _parser::parse, but aspectj NPEs with that currently
                                .named("createAndParseRequest");

                // Shapes
                final UniformFanOutShape<HttpRequest, HttpRequest> split = builder.add(Broadcast.create(2));

                final FlowShape<HttpRequest, byte[]> getBody = builder.add(getBodyFlow);
                final FlowShape<HttpRequest, Multimap<String, String>> getHeaders = builder.add(getHeadersFlow);
                final FanInShape2<
                        byte[],
                        Multimap<String, String>,
                        Pair<byte[], Multimap<String, String>>> join = builder.add(Zip.create());
                final FlowShape<Pair<byte[], Multimap<String, String>>, Record> createRequest =
                        builder.add(createAndParseFlow);

                // Wire the shapes
                builder.from(split.out(0)).via(getBody).toInlet(join.in0()); // Split to get the body bytes
                builder.from(split.out(1)).via(getHeaders).toInlet(join.in1()); // Split to get the headers
                builder.from(join.out()).toInlet(createRequest.in()); // Join to create the Request and parse it

                return new FlowShape<>(split.in(), createRequest.out());
            });
        }

        private static Multimap<String, String> createHeaderMultimap(final Iterable<HttpHeader> headers) {
            final ImmutableMultimap.Builder<String, String> headersBuilder = ImmutableMultimap.builder();

            for (final HttpHeader httpHeader : headers) {
                headersBuilder.put(httpHeader.lowercaseName(), httpHeader.value());
            }

            return headersBuilder.build();
        }

        private static com.arpnetworking.metrics.mad.model.HttpRequest mapModel(final Pair<byte[], Multimap<String, String>> pair) {
            return new com.arpnetworking.metrics.mad.model.HttpRequest(pair.second(), pair.first());
        }

        private List<Record> parseRecords(final com.arpnetworking.metrics.mad.model.HttpRequest request)
                throws ParsingException {
            return _parser.parse(request);
        }

        private final Sink<Record, CompletionStage<Done>> _sink;
        private final Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser;
        private final Materializer _materializer;
        private final Graph<FlowShape<HttpRequest, Record>, NotUsed> _processGraph;

        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(CollectdHttpSourceV1.class, Duration.ofSeconds(30));
    }

    /**
     * CollectdHttpSourceV1 {@link BaseSource.Builder} implementation.
     */
    public static class Builder extends ActorSource.Builder<Builder, CollectdHttpSourceV1> {
        /**
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Sets the parser to use to parse the data. Optional. Cannot be null.
         *
         * @param value Value
         * @return This builder
         */
        public Builder setParser(final Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> value) {
            _parser = value;
            return self();
        }
        /**
         * Public constructor.
         */
        public Builder() {
            super(CollectdHttpSourceV1::new);
        }

        @NotNull
        private Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser = new CollectdJsonToRecordParser();
    }
}
