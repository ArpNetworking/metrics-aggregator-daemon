/*
 * Copyright 2018 Inscope Metrics, Inc
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
import akka.io.Udp;
import akka.io.UdpMessage;
import akka.util.ByteString;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;

import java.net.InetSocketAddress;
import java.util.function.Function;

/**
 * Base source that receives data over udp. Subclasses should set appropriate
 * defaults on the abstract builder.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public abstract class BaseUdpSource extends ActorSource {

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected BaseUdpSource(final Builder<?, ?> builder) {
        super(builder);
        _host = builder._host;
        _port = builder._port;
    }

    private final String _host;
    private final int _port;

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseUdpSource.class);

    /**
     * Internal actor to process connections and data.
     */
    protected abstract static class BaseUdpSourceActor extends AbstractActor {

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
                                .setMessage("Udp server binding complete")
                                .addData("name", _source.getName())
                                .addData("address", updBound.localAddress().getAddress().getHostAddress())
                                .addData("port", updBound.localAddress().getPort())
                                .addData("socket", _socket)
                                .log();
                    })
                    .match(Udp.Received.class, updReceived -> {
                        final ByteString data = updReceived.data();

                        LOGGER.trace()
                                .setMessage("Udp received datagram")
                                .addData("name", _source.getName())
                                .addData("bytes", updReceived.data().size())
                                .addData("socket", _socket)
                                .log();

                        try {
                            processData(data);
                            // CHECKSTYLE.OFF: IllegalCatch - Ensure all exceptions are logged (this is top level)
                        } catch (final RuntimeException e) {
                            // CHECKSTYLE.ON: IllegalCatch
                            LOGGER.error()
                                    .setMessage("Error processing data")
                                    .addData("name", _source.getName())
                                    .addData("socket", _socket)
                                    .addData("data", data)
                                    .setThrowable(e)
                                    .log();
                        }
                    })
                    .matchEquals(UdpMessage.unbind(), message -> {
                        LOGGER.debug()
                                .setMessage("Udp unbind")
                                .addData("name", _source.getName())
                                .addData("socket", _socket)
                                .log();
                        _socket.tell(message, getSelf());
                    })
                    .match(Udp.Unbound.class, message -> {
                        LOGGER.debug()
                                .setMessage("Udp unbound")
                                .addData("name", _source.getName())
                                .addData("socket", _socket)
                                .log();
                        getContext().stop(getSelf());
                    })
                    .build();
        }

        protected BaseUdpSource getSource() {
            return _source;
        }

        protected ActorRef getSocket() {
            return _socket;
        }

        /**
         * Parse the received data. This method should not throw.
         *
         * @param data the received {@code ByteString} to process
         */
        protected abstract void processData(ByteString data);

        /**
         * Constructor.
         *
         * @param source The {@link BaseUdpSource} to send notifications through.
         */
        protected BaseUdpSourceActor(final BaseUdpSource source) {
            _source = source;
            _host = source._host;
            _port = source._port;

            final ActorRef udpManager = Udp.get(getContext().system()).getManager();
            udpManager.tell(
                    UdpMessage.bind(getSelf(), new InetSocketAddress(_host, _port)),
                    getSelf());
        }

        private final BaseUdpSource _source;
        private final String _host;
        private final int _port;
        private boolean _isReady = false;
        private ActorRef _socket;

        private static final String IS_READY = "IsReady";
    }

    /**
     * BaseUdpSource {@link Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public abstract static class Builder<B extends Builder<B, S>, S extends BaseUdpSource>
            extends ActorSource.Builder<B, S> {

        /**
         * Protected constructor.
         *
         * @param targetConstructor the concrete source constructor to build through
         */
        public Builder(final Function<B, S> targetConstructor) {
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
         * Sets the port to listen on. Optional. Cannot be null. Must be
         * between 1 and 65535 (inclusive). Default is 8125.
         *
         * @param value the port to listen on
         * @return This builder
         */
        public B setPort(final Integer value) {
            _port = value;
            return self();
        }

        @NotNull
        @NotEmpty
        private String _host = "localhost";
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _port;
    }
}
