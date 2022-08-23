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
package com.arpnetworking.metrics.common.sources;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
import akka.japi.Pair;
import akka.stream.ActorAttributes;
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
import com.arpnetworking.http.RequestReply;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.ImmutableMultimap;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Source that uses HTTP POSTs as input.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HttpSource extends ActorSource {

    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected HttpSource(final Builder<?, ? extends HttpSource> builder) {
        super(builder);
        _parser = builder._parser;
        _periodicMetrics = builder._periodicMetrics;
    }

    private final PeriodicMetrics _periodicMetrics;
    private final Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser;

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends AbstractActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link HttpSource} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final HttpSource source) {
            return Props.create(Actor.class, source);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(RequestReply.class, this::requestReply)
                    .build();
        }

        private void requestReply(final RequestReply requestReply) {
            // TODO(barp): Fix the ugly HttpRequest cast here due to java vs scala dsl
            akka.stream.javadsl.Source.single(requestReply.getRequest())
                    .log("http source stream failure")
                    .via(_processGraph)
                    .toMat(_sink, Keep.right())
                    .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()))
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
        }

        /**
         * Constructor.
         *
         * @param source The {@link HttpSource} to send notifications through.
         */
        /* package private */ Actor(final HttpSource source) {
            _periodicMetrics = source._periodicMetrics;
            _metricSafeName = source.getMetricSafeName();
            _parser = source._parser;
            _sink = Sink.foreach(source::notify);
            _materializer = Materializer.createMaterializer(context());

            _periodicMetrics.registerPolledMetric(m -> {
                // TODO(vkoskela): There needs to be a way to deregister these callbacks
                // This is not an immediate issue since new Aggregator instances are
                // only created when pipelines are reloaded. To avoid recording values
                // for dead pipelines this explicitly avoids recording zeroes.
                final long samples = _receivedSamples.getAndSet(0);
                final long requests = _receivedRequests.getAndSet(0);
                if (samples > 0) {
                    m.recordCounter(String.format("sources/http/%s/metric_samples", _metricSafeName), samples);
                }
                if (requests > 0) {
                    m.recordCounter(String.format("sources/http/%s/requests", _metricSafeName), requests);
                }
            });

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

        private static com.arpnetworking.metrics.mad.model.HttpRequest mapModel(
                final Pair<ByteString, ImmutableMultimap<String, String>> pair) {
            return new com.arpnetworking.metrics.mad.model.HttpRequest(pair.second(), pair.first());
        }

        private List<Record> parseRecords(final com.arpnetworking.metrics.mad.model.HttpRequest request)
                throws ParsingException {
            _receivedRequests.incrementAndGet();
            final List<Record> records = _parser.parse(request);
            long samples = 0;
            for (final Record record : records) {
                for (final Metric metric : record.getMetrics().values()) {
                    samples += metric.getValues().size();
                    final List<CalculatedValue<?>> countStatistic =
                            metric.getStatistics().get(STATISTIC_FACTORY.getStatistic("count"));
                    if (countStatistic != null) {
                        samples += countStatistic.stream()
                                .map(s -> s.getValue().getValue())
                                .reduce(Double::sum)
                                .orElse(0.0d);
                    }
                }
            }
            _receivedSamples.addAndGet(samples);

            return records;
        }

        // WARNING: Consider carefully the volume of samples recorded.
        // PeriodicMetrics reduces the number of scopes creates, but each sample is
        // still stored in-memory until it is flushed.
        private final PeriodicMetrics _periodicMetrics;

        private final String _metricSafeName;
        private final Sink<Record, CompletionStage<Done>> _sink;
        private final Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser;
        private final Materializer _materializer;
        private final Graph<FlowShape<HttpRequest, Record>, NotUsed> _processGraph;
        private final AtomicLong _receivedSamples = new AtomicLong(0);
        private final AtomicLong _receivedRequests = new AtomicLong(0);

        private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
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
        public B setParser(final Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> value) {
            _parser = value;
            return self();
        }

        /**
         * Sets the periodic metrics instance.
         *
         * @param value The periodic metrics.
         * @return This instance of {@link Builder}
         */
        public final B setPeriodicMetrics(final PeriodicMetrics value) {
            _periodicMetrics = value;
            return self();
        }

        @NotNull
        private Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser;

        @NotNull
        @JacksonInject
        private PeriodicMetrics _periodicMetrics;
    }
}
