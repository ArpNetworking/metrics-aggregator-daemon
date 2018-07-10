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
package com.inscopemetrics.mad.sources;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Tcp;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.inscopemetrics.mad.model.Record;
import com.inscopemetrics.mad.parsers.Parser;
import com.inscopemetrics.mad.parsers.exceptions.ParsingException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * Source that uses line delimited data over tcp.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public abstract class TcpLineSource extends BaseTcpSource {

    /**
     * Provide the {@link Parser} to use to convert a line of data to
     * a {@code List} of {@link Record} instances.
     *
     * @return the {@link Parser} to use
     */
    protected abstract Parser<List<Record>, ByteBuffer> getParser();

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected TcpLineSource(final Builder<?, ?> builder) {
        super(builder);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpLineSource.class);

    /**
     * Internal actor to accept connections.
     */
    static final class TcpListenerActor extends BaseTcpListenerActor {
        /**
         * Creates a {@code Props} for this actor.
         *
         * @param source The {@link TcpLineSource} to send notifications through.
         * @return A new {@code Props}
         */
        static Props props(final TcpLineSource source) {
            return Props.create(TcpListenerActor.class, source);
        }

        @Override
        protected ActorRef createHandler(final Tcp.Connected connected) {
            return getContext().actorOf(Props.create(
                    TcpDataHandlerActor.class,
                    getSource(),
                    connected.remoteAddress()));
        }

        /**
         * Constructor.
         *
         * @param source The {@link TcpLineSource} to send notifications through.
         */
        TcpListenerActor(final TcpLineSource source) {
            super(source);
        }
    }

    /**
     * Internal actor to process requests.
     */
    protected static final class TcpDataHandlerActor extends BaseTcpDataHandlerActor {

        /**
         * Protected constructor.
         *
         * @param source the source to bind the actor to
         * @param remoteAddress the remote address of the client
         */
        protected TcpDataHandlerActor(
                final TcpLineSource source,
                final InetSocketAddress remoteAddress) {
            super(source, remoteAddress);
            _parser = source.getParser();
        }

        @Override
        protected void processData(final ByteString data) {
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
                // TODO(ville): The parser should be an actor set.
                final List<Record> records = _parser.parse(data.toByteBuffer());

                LOGGER.debug()
                        .setMessage("Parsed records")
                        .addData("name", getSource().getName())
                        .addData("records", records.size())
                        .addData("remoteAddress", getRemoteAddress().getAddress().getHostAddress())
                        .addData("remotePort", getRemoteAddress().getPort())
                        .log();

                records.forEach(getSource()::notify);
            } catch (final ParsingException e) {
                BAD_REQUEST_LOGGER.warn()
                        .setMessage("Error processing records")
                        .addData("name", getSource().getName())
                        .addData("remoteAddress", getRemoteAddress().getAddress().getHostAddress())
                        .addData("remotePort", getRemoteAddress().getPort())
                        .setThrowable(e)
                        .log();
            }
        }

        private final ByteStringBuilder _buffer = new ByteStringBuilder();
        private final Parser<List<Record>, ByteBuffer> _parser;

        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(TcpLineSource.class, Duration.ofSeconds(30));
    }

    /**
     * TcpLineSource {@link Builder} implementation.
     *
     * @param <B> the builder type
     * @param <S> the source type
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public abstract static class Builder<B extends BaseTcpSource.Builder<B, S>, S extends TcpLineSource>
            extends BaseTcpSource.Builder<B, S> {

        /**
         * Protected constructor.
         *
         * @param targetConstructor the concrete source constructor to build through
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }
    }
}
