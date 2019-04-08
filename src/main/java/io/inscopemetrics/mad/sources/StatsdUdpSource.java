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

import akka.actor.Props;
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import io.inscopemetrics.mad.model.Record;
import io.inscopemetrics.mad.parsers.Parser;
import io.inscopemetrics.mad.parsers.StatsdToRecordParser;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Source for receiving Statsd data over UDP. By default:
 *
 * <ul>
 * <li>the source is bound to {@code localhost}</li>
 * <li>the source listens on {@code 8125}</li>
 * </ul>
 *
 * The behavior may be customized by setting fields through the Builder. An
 * example can be found in {@code config/pipelines/pipeline.conf}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class StatsdUdpSource extends UdpDatagramSource {

    @Override
    protected Props createProps() {
        return UdpDatagramSourceActor.props(this);
    }

    @Override
    protected Parser<List<Record>, ByteBuffer> getParser() {
        return _parser;
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    private StatsdUdpSource(final Builder builder) {
        super(builder);
        _parser = ThreadLocalBuilder.build(
                StatsdToRecordParser.Builder.class,
                b -> { });
    }

    private final Parser<List<Record>, ByteBuffer> _parser;

    /**
     * StatsdUdpSource {@link Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends UdpDatagramSource.Builder<Builder, StatsdUdpSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(StatsdUdpSource::new);
            setPort(DEFAULT_PORT);
            setName(DEFAULT_NAME);
        }

        @Override
        protected Builder self() {
            return this;
        }

        private static final int DEFAULT_PORT = 8125;
        private static final String DEFAULT_NAME = "statsd_udp_source";
    }
}
