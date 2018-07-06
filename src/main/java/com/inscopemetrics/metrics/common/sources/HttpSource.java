/*
 * Copyright 2016 Inscope Metrics, Inc
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
package com.inscopemetrics.metrics.common.sources;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
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
import akka.stream.javadsl.Zip;
import akka.util.ByteString;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableMultimap;
import com.inscopemetrics.http.RequestReply;
import com.inscopemetrics.metrics.common.parsers.Parser;
import com.inscopemetrics.metrics.common.parsers.exceptions.ParsingException;
import com.inscopemetrics.metrics.mad.model.Record;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Source that uses HTTP POSTs as input.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HttpSource extends ActorSource {

    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected HttpSource(final Builder<?, ? extends HttpSource> builder) {
        super(builder);
        _parser = builder._parser;
    }

    private final Parser<List<Record>, com.inscopemetrics.metrics.mad.model.HttpRequest> _parser;

    /**
     * Internal actor to process requests.
     */
    static final class Actor extends AbstractActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link HttpSource} to send notifications through.
         * @return A new {@link Props}
         */
        static Props props(final HttpSource source) {
            return Props.create(Actor.class, source);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(RequestReply.class, requestReply -> {
                        // TODO(barp): Fix the ugly HttpRequest cast here due to java vs scala dsl
                        akka.stream.javadsl.Source.single(requestReply.getRequest())
                                .via(_processGraph)
                                .toMat(_sink, Keep.right())
                                .run(_materializer)
                                .whenComplete((done, err) -> {
                                    final CompletableFuture<HttpResponse> responseFuture = requestReply.getResponse();
                                    if (err == null) {
                                        responseFuture.complete(HttpResponse.create().withStatus(200));
                                    } else {
                                        BAD_REQUEST_LOGGER.warn()
                                                .setMessage("Error handling http post")
                                                .setThrowable(err)
                                                .log();
                                        if (err instanceof ParsingException) {
                                            responseFuture.complete(HttpResponse.create().withStatus(400));
                                        } else {
                                            responseFuture.complete(HttpResponse.create().withStatus(500));
                                        }
                                    }
                                });
                    })
                    .build();
        }

        /**
         * Constructor.
         *
         * @param source The {@link HttpSource} to send notifications through.
         */
        Actor(final HttpSource source) {
            _parser = source._parser;
            _sink = Sink.foreach(source::notify);
            _materializer = ActorMaterializer.create(
                    ActorMaterializerSettings.create(context().system())
                            .withSupervisionStrategy(Supervision.stoppingDecider()),
                    context());

            _processGraph = GraphDSL.create(builder -> {

                // Flows
                final Flow<HttpRequest, ByteString, NotUsed> getBodyFlow = Flow.<HttpRequest>create()
                        .map(HttpRequest::entity)
                        .flatMapConcat(RequestEntity::getDataBytes)
                        .reduce(ByteString::concat)
                        .named("getBody");

                final Flow<HttpRequest, ImmutableMultimap<String, String>, NotUsed> getHeadersFlow = Flow.<HttpRequest>create()
                        .map(HttpRequest::getHeaders)
                        .map(Actor::createHeaderMultimap) // Transform to array form
                        .named("getHeaders");

                final Flow<Pair<ByteString, ImmutableMultimap<String, String>>, Record, NotUsed> createAndParseFlow =
                        Flow.<Pair<ByteString, ImmutableMultimap<String, String>>>create()
                                .map(Actor::mapModel)
                                .mapConcat(this::parseRecords) // Parse the json string into a record builder
                                // NOTE: this should be _parser::parse, but aspectj NPEs with that currently
                                .named("createAndParseRequest");

                // Shapes
                final UniformFanOutShape<HttpRequest, HttpRequest> split = builder.add(Broadcast.create(2));

                final FlowShape<HttpRequest, ByteString> getBody = builder.add(getBodyFlow);
                final FlowShape<HttpRequest, ImmutableMultimap<String, String>> getHeaders = builder.add(getHeadersFlow);
                final FanInShape2<
                        ByteString,
                        ImmutableMultimap<String, String>,
                        Pair<ByteString, ImmutableMultimap<String, String>>> join = builder.add(Zip.create());
                final FlowShape<Pair<ByteString, ImmutableMultimap<String, String>>, Record> createRequest =
                        builder.add(createAndParseFlow);

                // Wire the shapes
                builder.from(split.out(0)).via(getBody).toInlet(join.in0()); // Split to get the body bytes
                builder.from(split.out(1)).via(getHeaders).toInlet(join.in1()); // Split to get the headers
                builder.from(join.out()).toInlet(createRequest.in()); // Join to create the Request and parse it

                return new FlowShape<>(split.in(), createRequest.out());
            });
        }

        private static ImmutableMultimap<String, String> createHeaderMultimap(final Iterable<HttpHeader> headers) {
            final ImmutableMultimap.Builder<String, String> headersBuilder = ImmutableMultimap.builder();

            for (final HttpHeader httpHeader : headers) {
                headersBuilder.put(httpHeader.lowercaseName(), httpHeader.value());
            }

            return headersBuilder.build();
        }

        private static com.inscopemetrics.metrics.mad.model.HttpRequest mapModel(
                final Pair<ByteString, ImmutableMultimap<String, String>> pair) {
            return new com.inscopemetrics.metrics.mad.model.HttpRequest(pair.second(), pair.first());
        }

        private List<Record> parseRecords(final com.inscopemetrics.metrics.mad.model.HttpRequest request)
                throws ParsingException {
            return _parser.parse(request);
        }

        private final Sink<Record, CompletionStage<Done>> _sink;
        private final Parser<List<Record>, com.inscopemetrics.metrics.mad.model.HttpRequest> _parser;
        private final Materializer _materializer;
        private final Graph<FlowShape<HttpRequest, Record>, NotUsed> _processGraph;

        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(HttpSource.class, Duration.ofSeconds(30));
    }

    /**
     * HttpSource {@link BaseSource.Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     *
     * @author Brandon Arp (brandon dot arp at smartsheet dot com)
     */
    public abstract static class Builder<B extends Builder<B, S>, S extends HttpSource> extends ActorSource.Builder<B, S> {

        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor The constructor for the concrete type to be created by this builder.
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        /**
         * Sets the parser to use to parse the data. Required. Cannot be null.
         *
         * @param value Value
         * @return This builder
         */
        public B setParser(final Parser<List<Record>, com.inscopemetrics.metrics.mad.model.HttpRequest> value) {
            _parser = value;
            return self();
        }

        @NotNull
        private Parser<List<Record>, com.inscopemetrics.metrics.mad.model.HttpRequest> _parser;
    }
}
