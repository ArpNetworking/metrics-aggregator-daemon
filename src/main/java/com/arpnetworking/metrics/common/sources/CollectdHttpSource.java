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
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.parsers.CollectdJsonToRecordParser;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.Maps;
import net.sf.oval.ConstraintViolation;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.exception.ConstraintsViolatedException;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Processes HTTP posts from Collectd, extracts data and emits metrics.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class CollectdHttpSource extends ActorSource {
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
    protected CollectdHttpSource(final Builder builder) {
        super(builder);
        _parser = builder._parser;
    }

    private final Parser<List<DefaultRecord.Builder>> _parser;

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends UntypedActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link CollectdHttpSource} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final CollectdHttpSource source) {
            return Props.create(Actor.class, source);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onReceive(final Object message) throws Exception {
            if (message instanceof RequestReply) {
                final RequestReply request = (RequestReply) message;
                Source.single((HttpRequest) request.getRequest())
                        .via(_processGraph)
                        .toMat(_sink, Keep.right())
                        .run(_materializer)
                        .whenComplete((done, err) -> {
                            final CompletableFuture<HttpResponse> responseFuture = request.getResponse();
                            if (err == null) {
                                responseFuture.complete(HttpResponse.create().withStatus(200));
                            } else {
                                BAD_REQUEST_LOGGER.warn()
                                        .setMessage("Error handling collectd post")
                                        .setThrowable(err)
                                        .log();
                                if (err instanceof ParsingException) {
                                    responseFuture.complete(HttpResponse.create().withStatus(400));
                                } else if (err instanceof ConstraintsViolatedException) {
                                    final ConstraintsViolatedException violationException = (ConstraintsViolatedException) err;
                                    final StringBuilder errorMessages = new StringBuilder();
                                    for (final ConstraintViolation violation : violationException.getConstraintViolations()) {
                                        final String error = violation.getMessage();
                                        errorMessages.append(error.substring(error.indexOf('_') + 1)).append("\n");
                                    }
                                    final HttpResponse response = HttpResponse.create()
                                            .withStatus(400)
                                            .withEntity(errorMessages.toString());
                                    responseFuture.complete(response);
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
         * @param source The {@link CollectdHttpSource} to send notifications through.
         */
        /* package private */ Actor(final CollectdHttpSource source) {
            _parser = source._parser;
            _sink = Sink.foreach(source::notify);
            _materializer = ActorMaterializer.create(
                    ActorMaterializerSettings.create(context().system())
                            .withSupervisionStrategy(Supervision.stoppingDecider()),
                    context());

            _processGraph = GraphDSL.create(builder -> {

                // Flows
                final Flow<HttpRequest, List<DefaultRecord.Builder>, NotUsed> parseJsonFlow = Flow.<HttpRequest>create()
                        .map(HttpRequest::entity)
                        .flatMapConcat(RequestEntity::getDataBytes)
                        .map(ByteString::toArray) // Transform to array form
                        .map(this::parseRecords) // Parse the json string into a record builder
                        .named("parseJson");

                final Flow<HttpRequest, Map<String, String>, NotUsed> parseHeadersFlow = Flow.<HttpRequest>create()
                        .map(HttpRequest::getHeaders)
                        .map(Actor::extractTagHeaders)
                        .named("parseHeaders");

                final Flow<
                        Pair<Map<String, String>, List<DefaultRecord.Builder>>,
                        DefaultRecord.Builder,
                        NotUsed> applyTagsFlow = Flow.<Pair<Map<String, String>, List<DefaultRecord.Builder>>>create()
                                .flatMapConcat(Actor::mapExpandPair)
                                .map(Actor::applyTags)
                                .named("applyTags");

                final Flow<DefaultRecord.Builder, Record, NotUsed> buildFlow = Flow.<DefaultRecord.Builder>create()
                        .map(DefaultRecord.Builder::build);

                // Shapes
                final UniformFanOutShape<HttpRequest, HttpRequest> split = builder.add(Broadcast.create(2));
                final FlowShape<HttpRequest, List<DefaultRecord.Builder>> parseJson = builder.add(parseJsonFlow);
                final FlowShape<HttpRequest, Map<String, String>> parseHeaders = builder.add(parseHeadersFlow);
                final FanInShape2<
                        Map<String, String>,
                        List<DefaultRecord.Builder>,
                        Pair<Map<String, String>, List<DefaultRecord.Builder>>> join = builder.add(Zip.create());
                final FlowShape<
                        Pair<Map<String, String>, List<DefaultRecord.Builder>>,
                        DefaultRecord.Builder> applyTags = builder.add(applyTagsFlow);
                final FlowShape<DefaultRecord.Builder, Record> build = builder.add(buildFlow);

                // Wire the shapes
                builder.from(split.out(0)).via(parseHeaders).toInlet(join.in0());
                builder.from(split.out(1)).via(parseJson).toInlet(join.in1());
                builder.from(join.out()).via(applyTags).via(build);

                return new FlowShape<>(split.in(), build.out());
            });
        }

        private static DefaultRecord.Builder applyTags(final Pair<Map<String, String>, DefaultRecord.Builder> pair)
                throws Exception {
            final DefaultRecord.Builder recordBuilder = pair.second();
            final Map<String, String> annotations = pair.first();
            recordBuilder.setAnnotations(annotations);
            applyTag("service", annotations, recordBuilder::setService);
            applyTag("cluster", annotations, recordBuilder::setCluster);
            return recordBuilder;
        }

        private static Map<String, String> extractTagHeaders(final Iterable<HttpHeader> headers) throws Exception {
            final HashMap<String, String> m = Maps.newHashMap();
            for (final HttpHeader header : headers) {
                if (header.lowercaseName().startsWith("x-tag-")) {
                    m.put(header.lowercaseName().substring(6), header.value());
                }
            }
            return m;
        }

        private List<DefaultRecord.Builder> parseRecords(final byte[] data) throws ParsingException {
            return _parser.parse(data);
        }

        private static <A, B extends Collection<C>, C> Source<Pair<A, C>, NotUsed> mapExpandPair(final Pair<A, B> pair) {
            return Source.from(
                    pair.second()
                            .stream()
                            .map(element -> Pair.apply(pair.first(), element))
                            .collect(Collectors.toList()));
        }

        private static void applyTag(final String key, final Map<String, String> annotations, final Consumer<String> setter) {
            if (annotations.containsKey(key)) {
                setter.accept(annotations.get(key));
            }
        }

        private final Sink<Record, CompletionStage<Done>> _sink;
        private final Parser<List<DefaultRecord.Builder>> _parser;
        private final Materializer _materializer;
        private final Graph<FlowShape<HttpRequest, Record>, NotUsed> _processGraph;

        private static final Logger BAD_REQUEST_LOGGER = LoggerFactory.getRateLimitLogger(CollectdHttpSource.class, Duration.ofMinutes(1));
    }

    /**
     * CollectdHttpSource {@link BaseSource.Builder} implementation.
     */
    public static class Builder extends ActorSource.Builder<Builder, CollectdHttpSource> {
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
        public Builder setParser(final Parser<List<DefaultRecord.Builder>> value) {
            _parser = value;
            return self();
        }
        /**
         * Public constructor.
         */
        public Builder() {
            super(CollectdHttpSource::new);
        }

        @NotNull
        private Parser<List<DefaultRecord.Builder>> _parser = new CollectdJsonToRecordParser();
    }
}
