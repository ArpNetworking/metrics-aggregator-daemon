/*
 * Copyright 2017 Inscope Metrics, Inc
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

import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.metrics.mad.parsers.StatsdToRecordParser;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.fasterxml.jackson.annotation.JacksonInject;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.io.Udp;
import org.apache.pekko.io.UdpMessage;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Source that uses Statsd as input.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class StatsdSource extends ActorSource {

    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    private StatsdSource(final Builder builder) {
        super(builder);
        _host = builder._host;
        _port = builder._port;
        _periodicMetrics = builder._periodicMetrics;
    }

    private final String _host;
    private final int _port;
    private final PeriodicMetrics _periodicMetrics;

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsdSource.class);
    private static final Parser<List<Record>, ByteBuffer> PARSER = new StatsdToRecordParser();

    /**
     * Name of the actor created to receive the Statsd datagrams.
     */
    public static final String ACTOR_NAME = "statsd";

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends AbstractActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link StatsdSource} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final StatsdSource source) {
            return Props.create(Actor.class, () -> new Actor(source));
        }


        @Override
        public void preStart() throws Exception, Exception {
            super.preStart();

            final ActorRef udpManager = Udp.get(getContext().system()).getManager();
            udpManager.tell(
                    UdpMessage.bind(getSelf(), new InetSocketAddress(_host, _port)),
                    getSelf());

            _periodicMetrics.registerPolledMetric(m -> {
                // TODO(vkoskela): There needs to be a way to deregister these callbacks
                // This is not an immediate issue since new Aggregator instances are
                // only created when pipelines are reloaded. To avoid recording values
                // for dead pipelines this explicitly avoids recording zeroes.
                final long samples = _receivedSamples.getAndSet(0);
                final long packets = _receivedPackets.getAndSet(0);
                if (samples > 0) {
                    m.recordCounter(String.format("sources/statsd/%s/metric_samples", _metricSafeName), samples);
                }
                if (packets > 0) {
                    m.recordCounter(String.format("sources/statsd/%s/packets", _metricSafeName), packets);
                }
            });
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchEquals(IS_READY, message -> {
                        getSender().tell(_isReady, getSelf());
                    })
                    .match(Udp.Bound.class, updBound -> {
                        _socket = getSender();
                        _isReady = true;
                        LOGGER.info()
                                .setMessage("Statsd server binding complete")
                                .addData("address", updBound.localAddress().getAddress().getHostAddress())
                                .addData("port", updBound.localAddress().getPort())
                                .addData("socket", _socket)
                                .log();
                    })
                    .match(Udp.Received.class, this::updReceived)
                    .matchEquals(UdpMessage.unbind(), message -> {
                        LOGGER.debug()
                                .setMessage("Statsd unbind")
                                .addData("socket", _socket)
                                .log();
                        _socket.tell(message, getSelf());
                    })
                    .match(Udp.Unbound.class, message -> {
                        LOGGER.debug()
                                .setMessage("Statsd unbound")
                                .addData("socket", _socket)
                                .log();
                        getContext().stop(getSelf());
                    })
                    .build();
        }

        private void updReceived(final Udp.Received updReceived) {
            LOGGER.trace()
                    .setMessage("Statsd received datagram")
                    .addData("bytes", updReceived.data().size())
                    .addData("socket", _socket)
                    .log();

            try {
                // NOTE: The parsing occurs in the actor itself which can become a bottleneck
                // if there are more records to be parsed then a single thread can handle.
                final List<Record> records = PARSER.parse(updReceived.data().toByteBuffer());
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
                _receivedPackets.addAndGet(1);
                records.forEach(_sink::notify);
            } catch (final ParsingException e) {
                BAD_REQUEST_LOGGER.warn()
                        .setMessage("Error handling statsd datagram")
                        .addData("socket", _socket)
                        .setThrowable(e)
                        .log();
            }
        }

        /**
         * Constructor.
         *
         * @param source The {@link StatsdSource} to send notifications through.
         */
        /* package private */ Actor(final StatsdSource source) {
            _sink = source;
            _host = source._host;
            _port = source._port;
            _periodicMetrics = source._periodicMetrics;
            _metricSafeName = source.getMetricSafeName();
        }

        private boolean _isReady = false;
        private ActorRef _socket;
        private final StatsdSource _sink;
        private final String _host;
        private final int _port;

        private static final String IS_READY = "IsReady";
        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(StatsdSource.class, Duration.ofSeconds(30));
        private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
        private final AtomicLong _receivedSamples = new AtomicLong(0);
        private final AtomicLong _receivedPackets = new AtomicLong(0);
        private final PeriodicMetrics _periodicMetrics;
        private final String _metricSafeName;
    }

    /**
     * StatsdSource {@link BaseSource.Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends ActorSource.Builder<Builder, StatsdSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(StatsdSource::new);
            setActorName(ACTOR_NAME);
        }

        /**
         * Sets the host to bind to. Optional. Cannot be null or empty.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public Builder setHost(final String value) {
            _host = value;
            return self();
        }

        /**
         * Sets the port to listen on. Optional. Cannot be null. Must be
         * between 1 and 65535 (inclusive). Default is 8125.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public Builder setPort(final Integer value) {
            _port = value;
            return self();
        }

        /**
         * Sets the periodic metrics instance.
         *
         * @param value The periodic metrics.
         * @return This instance of {@link HttpSource.Builder}
         */
        public Builder setPeriodicMetrics(final PeriodicMetrics value) {
            _periodicMetrics = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        @NotEmpty
        private String _host = "localhost";
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _port = 8125;
        @NotNull
        @JacksonInject
        private PeriodicMetrics _periodicMetrics;
    }
}
