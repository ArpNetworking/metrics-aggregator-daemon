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

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.io.Tcp;
import org.apache.pekko.util.ByteString;
import org.apache.pekko.util.ByteStringBuilder;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import net.sf.oval.constraint.NotNull;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

/**
 * Source that uses line delimited data over tcp.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class TcpLineSource extends BaseTcpSource {

    @Override
    protected Props createProps() {
        return TcpListenerActor.props(this);
    }

    /* package private */ Parser<List<Record>, ByteBuffer> getParser() {
        return _parser;
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    private TcpLineSource(final Builder builder) {
        super(builder);
        _parser = builder._parser;
    }

    private final Parser<List<Record>, ByteBuffer> _parser;

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpLineSource.class);

    /**
     * Internal actor to processRecords requests.
     */
    /* package private */ static final class TcpListenerActor extends BaseTcpListenerActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link TcpLineSource} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final TcpLineSource source) {
            return Props.create(TcpListenerActor.class, source);
        }

        @Override
        protected ActorRef createHandler(final BaseTcpSource source, final Tcp.Connected connected) {
            return getContext().actorOf(Props.create(
                    TcpRequestHandlerActor.class,
                    getSink(),
                    connected.remoteAddress()));
        }

        /**
         * Constructor.
         *
         * @param source The {@link TcpLineSource} to send notifications through.
         */
        /* package private */ TcpListenerActor(final TcpLineSource source) {
            super(source);
        }
    }

    /**
     * Internal actor to processRecords requests.
     */
    /* package private */ static final class TcpRequestHandlerActor extends AbstractActor {

        /* package private */ TcpRequestHandlerActor(
                final TcpLineSource sink,
                final InetSocketAddress remoteAddress) {
            _sink = sink;
            _remoteAddress = remoteAddress;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Tcp.Received.class, this::tcpReceived)
                    .match(Tcp.ConnectionClosed.class, message -> {
                        getContext().stop(getSelf());
                        LOGGER.debug()
                                .setMessage("Tcp connection close")
                                .addData("name", _sink.getName())
                                .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                                .addData("remotePort", _remoteAddress.getPort())
                                .log();
                    })
                    .build();
        }

        private void tcpReceived(final Tcp.Received message) {
            final ByteString data = message.data();

            LOGGER.trace()
                    .setMessage("Tcp data received")
                    .addData("name", _sink.getName())
                    .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                    .addData("remotePort", _remoteAddress.getPort())
                    .addData("data", data)
                    .log();

            try {
                processData(data);
                // CHECKSTYLE.OFF: IllegalCatch - Ensure all exceptions are logged (this is top level)
            } catch (final RuntimeException e) {
                // CHECKSTYLE.ON: IllegalCatch
                BAD_REQUEST_LOGGER.warn()
                        .setMessage("Error processing data")
                        .addData("name", _sink.getName())
                        .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                        .addData("remotePort", _remoteAddress.getPort())
                        .addData("data", data)
                        .setThrowable(e)
                        .log();
            }
        }

        private void processData(final ByteString data) {
            // Process buffer line by line buffering any partial lines
            int indexStart = 0;
            int indexEnd = data.indexOf('\n');

            // It is possible for there to be more than one line in the new data
            while (indexEnd >= 0) {
                // Append the rest of a line to the buffer and processRecords the buffer
                _buffer.append(data.slice(indexStart, indexEnd));
                processRecords(_buffer.result());
                _buffer.clear();

                // Check if the datagram contains additional data
                if (indexEnd + 1 < data.size()) {
                    // More data
                    indexStart = indexEnd + 1;
                    indexEnd = data.indexOf('\n', indexStart);
                } else {
                    // No more data
                    indexStart = -1;
                    indexEnd = -1;
                }
            }

            // Append any remaining data to the buffer which (does not include a newline)
            if (indexStart >= 0) {
                _buffer.append(data.slice(indexStart, data.size()));
            }
        }

        private void processRecords(final ByteString data) {
            try {
                // NOTE: The parsing occurs in the actor itself which can become a bottleneck
                // if there are more records to be parsed then a single thread can handle.
                final List<Record> records = _sink.getParser().parse(data.toByteBuffer());

                LOGGER.trace()
                        .setMessage("Parsed records")
                        .addData("name", _sink.getName())
                        .addData("records", records.size())
                        .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                        .addData("remotePort", _remoteAddress.getPort())
                        .log();

                records.forEach(_sink::notify);
            } catch (final ParsingException e) {
                BAD_REQUEST_LOGGER.warn()
                        .setMessage("Error processing records")
                        .addData("name", _sink.getName())
                        .addData("remoteAddress", _remoteAddress.getAddress().getHostAddress())
                        .addData("remotePort", _remoteAddress.getPort())
                        .setThrowable(e)
                        .log();
            }
        }

        private final ByteStringBuilder _buffer = new ByteStringBuilder();
        private final TcpLineSource _sink;
        private final InetSocketAddress _remoteAddress;

        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(TcpLineSource.class, Duration.ofSeconds(30));
    }

    /**
     * TcpLineSource {@link BaseSource.Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends BaseTcpSource.Builder<Builder, TcpLineSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TcpLineSource::new);
        }

        /**
         * Set the parser. Required. Cannot be null.
         *
         * @param value the parser
         * @return this {@link Builder} instance
         */
        public Builder setParser(final Parser<List<Record>, ByteBuffer> value) {
            _parser = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Parser<List<Record>, ByteBuffer> _parser;
    }
}
