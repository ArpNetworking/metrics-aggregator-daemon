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
package io.inscopemetrics.mad.sources;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;

import java.net.InetSocketAddress;
import java.util.function.Function;

/**
 * Base source that listens on a tcp port. Subclasses should set appropriate
 * defaults on the abstract builder.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public abstract class BaseTcpSource extends ActorSource {

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected BaseTcpSource(final Builder<?, ?> builder) {
        super(builder);
        _host = builder._host;
        _port = builder._port;
        _listenQueue = builder._listenQueue;
    }

    private final String _host;
    private final int _port;
    private final int _listenQueue;

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTcpSource.class);

    /**
     * Internal base actor to accept connections.
     */
    protected abstract static class BaseTcpListenerActor extends AbstractActor {
        /**
         * Creates a {@code Props} for this actor.
         *
         * @param source The {@link BaseTcpSource} to send notifications through.
         * @return A new {@code Props}
         */
        protected static Props props(final BaseTcpSource source) {
            return Props.create(BaseTcpListenerActor.class, source);
        }

        @Override
        public void preStart() {
            final ActorRef tcpManager = Tcp.get(getContext().system()).manager();
            tcpManager.tell(
                    TcpMessage.bind(
                            getSelf(),
                            new InetSocketAddress(_host, _port),
                            _listenQueue),
                    getSelf());
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchEquals(IS_READY, message -> {
                        getSender().tell(_isReady, getSelf());
                    })
                    .match(Tcp.Bound.class, tcpBound -> {
                        LOGGER.info()
                                .setMessage("Tcp server binding complete")
                                .addData("name", _source.getName())
                                .addData("address", tcpBound.localAddress().getAddress().getHostAddress())
                                .addData("port", tcpBound.localAddress().getPort())
                                .log();

                        _isReady = true;
                    })
                    .match(Tcp.CommandFailed.class, failed -> {
                        LOGGER.warn()
                                .setMessage("Tcp server bad command")
                                .addData("name", _source.getName())
                                .log();

                        getContext().stop(getSelf());
                    })
                    .match(Tcp.Connected.class, tcpConnected -> {
                        LOGGER.debug()
                                .setMessage("Tcp connection established")
                                .addData("name", _source.getName())
                                .addData("remoteAddress", tcpConnected.remoteAddress().getAddress().getHostAddress())
                                .addData("remotePort", tcpConnected.remoteAddress().getPort())
                                .log();

                        final ActorRef handler = createHandler(tcpConnected);
                        getSender().tell(TcpMessage.register(handler), getSelf());
                    })
                    .build();
        }

        /**
         * Abstract method to create tcp message actor instance for each connection.
         *
         * @param connected the connected message
         * @return the actor reference
         */
        protected abstract ActorRef createHandler(Tcp.Connected connected);

        protected BaseTcpSource getSource() {
            return _source;
        }

        /**
         * Constructor.
         *
         * @param source The {@link BaseTcpSource} to send notifications through.
         */
        protected BaseTcpListenerActor(final BaseTcpSource source) {
            _source = source;
            _host = source._host;
            _port = source._port;
            _listenQueue = source._listenQueue;
        }

        private boolean _isReady = false;
        private final BaseTcpSource _source;
        private final String _host;
        private final int _port;
        private final int _listenQueue;

        private static final String IS_READY = "IsReady";
    }

    /**
     * Internal base actor to process data.
     */
    protected abstract static class BaseTcpDataHandlerActor extends AbstractActor {

        /**
         * Protected constructor.
         *
         * @param source the source to bind the actor to
         * @param remoteAddress the remote address of the client
         */
        protected BaseTcpDataHandlerActor(
                final BaseTcpSource source,
                final InetSocketAddress remoteAddress) {
            _source = source;
            _remoteAddress = remoteAddress;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Tcp.Received.class, message -> {
                        final ByteString data = message.data();

                        LOGGER.trace()
                                .setMessage("Tcp data received")
                                .addData("name", _source.getName())
                                .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                                .addData("remotePort", _remoteAddress.getPort())
                                .addData("data", data)
                                .log();

                        try {
                            processData(data);
                            // CHECKSTYLE.OFF: IllegalCatch - Ensure all exceptions are logged (this is top level)
                        } catch (final RuntimeException e) {
                            // CHECKSTYLE.ON: IllegalCatch
                            LOGGER.error()
                                    .setMessage("Error processing data")
                                    .addData("name", _source.getName())
                                    .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                                    .addData("remotePort", _remoteAddress.getPort())
                                    .addData("data", data)
                                    .setThrowable(e)
                                    .log();
                        }
                    })
                    .match(Tcp.ConnectionClosed.class, message -> {
                        getContext().stop(getSelf());
                        LOGGER.debug()
                                .setMessage("Tcp connection close")
                                .addData("name", _source.getName())
                                .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                                .addData("remotePort", _remoteAddress.getPort())
                                .log();
                    })
                    .build();
        }

        protected BaseTcpSource getSource() {
            return _source;
        }

        protected InetSocketAddress getRemoteAddress() {
            return _remoteAddress;
        }

        /**
         * Parse and act on the received data. This method should not throw.
         *
         * @param data the received {@code ByteString} to process
         */
        protected abstract void processData(ByteString data);

        private final BaseTcpSource _source;
        private final InetSocketAddress _remoteAddress;
    }

    /**
     * BaseTcpSource {@link Builder} implementation.
     *
     * @param <B> the builder type
     * @param <S> the source type
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public abstract static class Builder<B extends Builder<B, S>, S extends BaseTcpSource>
            extends ActorSource.Builder<B, S> {

        /**
         * Protected constructor.
         *
         * @param targetConstructor the concrete source constructor to build through
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        /**
         * Sets the host to bind to. Optional. Cannot be null or empty.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public B setHost(final String value) {
            _host = value;
            return self();
        }

        /**
         * Sets the port to listen on. Required. Cannot be null. Must be
         * between 1 and 65535 (inclusive). Subclasses may set a default
         * port, in which case this field is effectively optional.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public B setPort(final Integer value) {
            _port = value;
            return self();
        }

        /**
         * Sets the listen queue length. Optional. Default is 100. Cannot be
         * null and must be at least 0.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public B setListenQueue(final Integer value) {
            _listenQueue = value;
            return self();
        }

        @NotNull
        @NotEmpty
        private String _host = "localhost";
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _port;
        @NotNull
        @Min(0)
        private Integer _listenQueue = 100;
    }
}
