/**
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

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.parsers.TelegrafJsonToRecordParser;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Source that uses Telegraf TCP SocketWriter with JSON as input. This sink
 * merges the name with the field using Telegraf's standard '.' (period)
 * separator. You can wrap this sink with the MappingSource in order to convert
 * to '/' (slash) delimited metric names.
 *
 * Sample MAD configuration:
 * <pre>
 * {
 *   type="com.arpnetworking.metrics.mad.sources.MappingSource"
 *   name="telegraftcp_mapping_source"
 *   findAndReplace={
 *     "\\."=["/"]
 *   }
 *   source={
 *     type="com.arpnetworking.metrics.common.sources.TelegrafTcpSource"
 *     name="telegraftcp_source"
 *     host="0.0.0.0"
 *     port="8094"
 *   }
 * }
 * </pre>
 *
 * Sample Telegraf configuration:
 * <pre>
 * [agent]
 * interval="1s"
 * flush_interval="1s"
 * round_interval=true
 * omit_hostname=false
 *
 * [global_tags]
 * service="telegraf"
 * cluster="telegraf_local"
 *
 * [[outputs.socket_writer]]
 * address = "tcp://127.0.0.1:8094"
 * data_format = "json"
 *
 * [[inputs.cpu]]
 * percpu = true
 * totalcpu = true
 * collect_cpu_time = false
 * report_active = false
 * </pre>
 *
 * TODO(ville): Parameterize the parser to use.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TelegrafTcpSource extends ActorSource {

    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    private TelegrafTcpSource(final Builder builder) {
        super(builder);
        _host = builder._host;
        _port = builder._port;
        _acceptQueue = builder._acceptQueue;
    }

    private final String _host;
    private final int _port;
    private final int _acceptQueue;

    private static final Logger LOGGER = LoggerFactory.getLogger(TelegrafTcpSource.class);
    private static final Parser<List<Record>, ByteBuffer> PARSER = new TelegrafJsonToRecordParser.Builder().build();

    /**
     * Name of the actor created to receive the Telegraf JSON.
     */
    public static final String ACTOR_NAME = "telegraf-tcp-json";

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends UntypedActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link TelegrafTcpSource} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final TelegrafTcpSource source) {
            return Props.create(Actor.class, source);
        }

        @Override
        public void preStart() {
            _tcpManager.tell(
                    TcpMessage.bind(
                            getSelf(),
                            new InetSocketAddress(_host, _port),
                            _acceptQueue),
                    getSelf());
        }

        @Override
        public void onReceive(final Object message) throws Exception {
            if (Objects.equals(IS_READY, message)) {
                getSender().tell(_isReady, getSelf());
            } else if (message instanceof Tcp.Bound) {
                final Tcp.Bound tcpBound = (Tcp.Bound) message;
                _isReady = true;
                _tcpManager.tell(message, getSelf());
                LOGGER.info()
                        .setMessage("Telegraf tcp server binding complete")
                        .addData("address", tcpBound.localAddress().getAddress().getHostAddress())
                        .addData("port", tcpBound.localAddress().getPort())
                        .log();
            } else if (message instanceof Tcp.CommandFailed) {
                getContext().stop(getSelf());
                LOGGER.warn()
                        .setMessage("Telegraf tcp server bad command")
                        .log();
            } else if (message instanceof Tcp.Connected) {
                final Tcp.Connected tcpConnected = (Tcp.Connected) message;
                _tcpManager.tell(message, getSelf());
                LOGGER.debug()
                        .setMessage("Telegraf tcp connection established")
                        .addData("remoteAddress", tcpConnected.remoteAddress().getAddress().getHostAddress())
                        .addData("remotePort", tcpConnected.remoteAddress().getPort())
                        .log();

                final ActorRef handler = getContext().actorOf(Props.create(
                        RequestHandlerActor.class,
                        _sink,
                        tcpConnected.remoteAddress()));
                getSender().tell(TcpMessage.register(handler), getSelf());
            } else {
                unhandled(message);
            }
        }

        /**
         * Constructor.
         *
         * @param source The {@link TelegrafTcpSource} to send notifications through.
         */
        /* package private */ Actor(final TelegrafTcpSource source) {
            _sink = source;
            _host = source._host;
            _port = source._port;
            _acceptQueue = source._acceptQueue;

            _tcpManager = Tcp.get(getContext().system()).manager();
        }

        private boolean _isReady = false;
        private final TelegrafTcpSource _sink;
        private final String _host;
        private final int _port;
        private final int _acceptQueue;
        private final ActorRef _tcpManager;

        private static final String IS_READY = "IsReady";
    }

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class RequestHandlerActor extends UntypedActor {

        /* package private */ RequestHandlerActor(final TelegrafTcpSource sink, final InetSocketAddress remoteAddress) {
            _sink = sink;
            _remoteAddress = remoteAddress;
        }

        @Override
        public void onReceive(final Object message) throws Throwable {
            if (message instanceof Tcp.Received) {
                final ByteString data = ((Tcp.Received) message).data();
                final int indexOfNewline = data.indexOf('\012');
                if (indexOfNewline >= 0) {
                    _buffer.append(data.slice(0, indexOfNewline));
                    process(_buffer.result());
                    _buffer.clear();
                    _buffer.append(data.slice(indexOfNewline + 1, data.size() - 1));
                } else {
                    _buffer.append(data);
                }

                LOGGER.trace()
                        .setMessage("Telegraf tcp data received")
                        .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                        .addData("remotePort", _remoteAddress.getPort())
                        .addData("data", data)
                        .log();
            } else if (message instanceof Tcp.ConnectionClosed) {
                getContext().stop(getSelf());
                LOGGER.debug()
                        .setMessage("Telegraf tcp connection close")
                        .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                        .addData("remotePort", _remoteAddress.getPort())
                        .log();
            }
        }

        private void process(final ByteString data) {
            try {
                // NOTE: The parsing occurs in the actor itself which can become a bottleneck
                // if there are more records to be parsed then a single thread can handle.
                final List<Record> records = PARSER.parse(data.toByteBuffer());
                records.forEach(_sink::notify);
            } catch (final ParsingException e) {
                BAD_REQUEST_LOGGER.warn()
                        .setMessage("Error handling telegraph tcp json")
                        .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                        .addData("remotePort", _remoteAddress.getPort())
                        .setThrowable(e)
                        .log();
            }
        }

        private final ByteStringBuilder _buffer = new ByteStringBuilder();
        private final TelegrafTcpSource _sink;
        private final InetSocketAddress _remoteAddress;

        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(TelegrafTcpSource.class, Duration.ofSeconds(30));
    }

    /**
     * TelegrafTcpSource {@link BaseSource.Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends ActorSource.Builder<Builder, TelegrafTcpSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TelegrafTcpSource::new);
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
         * between 1 and 65535 (inclusive). Default is 8094.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public Builder setPort(final Integer value) {
            _port = value;
            return self();
        }

        /**
         * Sets the accept queue length. Optional. Cannot be null. Must be at
         * least 0. Default is 100.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public Builder setAcceptQueue(final Integer value) {
            _acceptQueue = value;
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
        private Integer _port = 8094;
        @NotNull
        @Min(0)
        private Integer _acceptQueue = 100;
    }
}
