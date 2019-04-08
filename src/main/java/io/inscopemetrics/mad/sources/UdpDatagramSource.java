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

import akka.actor.Props;
import akka.util.ByteString;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import io.inscopemetrics.mad.model.Record;
import io.inscopemetrics.mad.parsers.Parser;
import io.inscopemetrics.mad.parsers.exceptions.ParsingException;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * Source that uses datagram data over udp.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public abstract class UdpDatagramSource extends BaseUdpSource {

    /**
     * Provide the {@link Parser} to use to convert a datagram to
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
    protected UdpDatagramSource(final Builder<?, ?> builder) {
        super(builder);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(UdpDatagramSource.class);

    /**
     * Internal actor to process connections and data.
     */
    protected static final class UdpDatagramSourceActor extends BaseUdpSource.BaseUdpSourceActor {
        /**
         * Creates a {@code Props} for this actor.
         *
         * @param source the {@link UdpDatagramSource} to send notifications through
         * @return A new {@code Props}
         */
        static Props props(final UdpDatagramSource source) {
            return Props.create(UdpDatagramSourceActor.class, source);
        }

        @Override
        protected void processData(final ByteString data) {
            try {
                // NOTE: The parsing occurs in the actor itself which can become a bottleneck
                // if there are more records to be parsed then a single thread can handle.
                // TODO(ville): The parser should be an actor set.
                final List<Record> records = _parser.parse(data.toByteBuffer());

                LOGGER.trace()
                        .setMessage("Parsed records")
                        .addData("name", getSource().getName())
                        .addData("records", records.size())
                        .addData("socket", getSocket())
                        .log();

                records.forEach(getSource()::notify);
            } catch (final ParsingException e) {
                BAD_REQUEST_LOGGER.warn()
                        .setMessage("Error parsing records")
                        .addData("name", getSource().getName())
                        .addData("socket", getSocket())
                        .setThrowable(e)
                        .log();
            }
        }

        /**
         * Constructor.
         *
         * @param source the {@link UdpDatagramSource} to send notifications through
         */
        protected UdpDatagramSourceActor(final UdpDatagramSource source) {
            super(source);
            _parser = source.getParser();
        }

        private final Parser<List<Record>, ByteBuffer> _parser;

        private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(UdpDatagramSource.class, Duration.ofSeconds(30));
    }

    /**
     * UdpDatagramSource {@link Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public abstract static class Builder<B extends Builder<B, S>, S extends UdpDatagramSource>
            extends BaseUdpSource.Builder<B, S> {

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
