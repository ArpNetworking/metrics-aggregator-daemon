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
package com.inscopemetrics.mad.sources;

import akka.actor.Props;
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.google.common.collect.ImmutableMap;
import com.inscopemetrics.mad.model.Record;
import com.inscopemetrics.mad.parsers.GraphitePlaintextToRecordParser;
import com.inscopemetrics.mad.parsers.Parser;
import net.sf.oval.constraint.NotNull;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Source for receiving Graphite plaintext data over TCP. By default:
 *
 * <ul>
 * <li>the source is bound to {@code localhost}</li>
 * <li>the source listens on port {@code 2003}</li>
 * <li>the source listen queue size is {@code 100}</li>
 * <li>the parser does not inject any global tags</li>
 * <li>the praser does not parse carbon tags</li>
 * </ul>
 *
 * The behavior may be customized by setting fields through the Builder. An
 * example can be found in {@code config/pipelines/pipeline.conf}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class GraphitePlaintextTcpSource extends TcpLineSource {

    @Override
    protected Props createProps() {
        return TcpListenerActor.props(this);
    }

    @Override
    protected Parser<List<Record>, ByteBuffer> getParser() {
        return _parser;
    }

    private GraphitePlaintextTcpSource(final Builder builder) {
        super(builder);
        _parser = ThreadLocalBuilder.build(
                GraphitePlaintextToRecordParser.Builder.class,
                b -> b.setGlobalTags(builder._globalTags)
                    .setParseCarbonTags(builder._parseCarbonTags));
    }

    private final Parser<List<Record>, ByteBuffer> _parser;

    /**
     * GraphitePlaintextTcpSource {@code Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends TcpLineSource.Builder<Builder, GraphitePlaintextTcpSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(GraphitePlaintextTcpSource::new);
            setPort(DEFAULT_PORT);
            setName(DEFAULT_NAME);
        }

        /**
         * Set global tags. Optional. Cannot be null. Default is an empty map.
         *
         * @param value the global tags
         * @return this {@link GraphitePlaintextToRecordParser.Builder} instance
         */
        public Builder setGlobalTags(final ImmutableMap<String, String> value) {
            _globalTags = value;
            return this;
        }

        /**
         * Set parse carbon tags. Optional. Cannot be null. Default is false.
         *
         * See: http://graphite.readthedocs.io/en/latest/tags.html
         *
         * @param value whether to parse carbon tags
         * @return this {@link GraphitePlaintextToRecordParser.Builder} instance
         */
        public Builder setParseCarbonTags(final Boolean value) {
            _parseCarbonTags = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private ImmutableMap<String, String> _globalTags = ImmutableMap.of();
        @NotNull
        private Boolean _parseCarbonTags = false;

        private static final int DEFAULT_PORT = 2003;
        private static final String DEFAULT_NAME = "graphite_plaintext_tcp_source";
    }
}
