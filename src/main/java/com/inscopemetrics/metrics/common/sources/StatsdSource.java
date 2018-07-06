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
package com.inscopemetrics.metrics.common.sources;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Udp;
import akka.io.UdpMessage;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.inscopemetrics.metrics.common.parsers.Parser;
import com.inscopemetrics.metrics.common.parsers.exceptions.ParsingException;
import com.inscopemetrics.metrics.mad.model.Record;
import com.inscopemetrics.metrics.mad.parsers.StatsdToRecordParser;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

/**
 * Source that uses Statsd as input.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class StatsdSource extends ActorSource {

    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    private StatsdSource(final Builder builder) {
        super(builder);
        _host = builder._host;
        _port = builder._port;
    }

    private final String _host;
    private final int _port;

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsdSource.class);
    private static final Parser<List<Record>, ByteBuffer> PARSER = new StatsdToRecordParser();

    /**
     * Name of the actor created to receive the Statsd datagrams.
     */
    public static final String ACTOR_NAME = "statsd";

    /**
     * Internal actor to process requests.
     */
    static final class Actor extends AbstractActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link StatsdSource} to send notifications through.
         * @return A new {@link Props}
         */
        static Props props(final StatsdSource source) {
            return Props.create(Actor.class, source);
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
                    .match(Udp.Received.class, updReceived -> {
                        LOGGER.trace()
                                .setMessage("Statsd received datagram")
                                .addData("bytes", updReceived.data().size())
                                .addData("socket", _socket)
                                .log();

                        try {
                            // NOTE: The parsing occurs in the actor itself which can become a bottleneck
                            // if there are more records to be parsed then a single thread can handle.
                            final List<Record> records = PARSER.parse(updReceived.data().toByteBuffer());
                            records.forEach(_sink::notify);
                        } catch (final ParsingException e) {
                            BAD_REQUEST_LOGGER.warn()
                                    .setMessage("Error handling statsd datagram")
                                    .addData("socket", _socket)
                                    .setThrowable(e)
                                    .log();
                        }
                    })
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

        /**
         * Constructor.
         *
         * @param source The {@link StatsdSource} to send notifications through.
         */
        Actor(final StatsdSource source) {
            _sink = source;
            _host = source._host;
            _port = source._port;

            final ActorRef udpManager = Udp.get(getContext().system()).getManager();
            udpManager.tell(
                    UdpMessage.bind(getSelf(), new InetSocketAddress(_host, _port)),
                    getSelf());
        }

        private boolean _isReady = false;
        private ActorRef _socket;
        private final StatsdSource _sink;
        private final String _host;
        private final int _port;

        private static final String IS_READY = "IsReady";
        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(StatsdSource.class, Duration.ofSeconds(30));
    }

    /**
     * StatsdSource {@link BaseSource.Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
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
    }
}
