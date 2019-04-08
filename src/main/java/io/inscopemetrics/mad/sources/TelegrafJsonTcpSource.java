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
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import io.inscopemetrics.mad.model.Record;
import io.inscopemetrics.mad.model.telegraf.TimestampUnit;
import io.inscopemetrics.mad.parsers.Parser;
import io.inscopemetrics.mad.parsers.TelegrafJsonToRecordParser;
import net.sf.oval.constraint.NotNull;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Source for receiving TelegrafJson JSON data over TCP. By default:
 *
 * <ul>
 * <li>the source is bound to {@code localhost}</li>
 * <li>the source listens on port {@code 8094}</li>
 * <li>the source listen queue size is {@code 100}</li>
 * <li>the parser uses seconds for the timestamp</li>
 * </ul>
 *
 * The behavior may be customized by setting fields through the Builder. An
 * example can be found in {@code config/pipelines/pipeline.conf}.
 *
 * Sample TelegrafJson configuration:
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
 * json_timestamp_units = "1s"
 * keep_alive_period = "5m"
 *
 * [[inputs.cpu]]
 * percpu = true
 * totalcpu = true
 * collect_cpu_time = false
 * report_active = false
 * </pre>
 *
 * The value `json_timestamp_units` in the TelegrafJson configuration must match the
 * units configured on this source.
 *
 * For more information please see:
 *
 * https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#json
 *
 * And:
 *
 * https://github.com/influxdata/telegraf/tree/master/plugins/outputs/socket_writer
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class TelegrafJsonTcpSource extends TcpLineSource {

    @Override
    protected Props createProps() {
        return TcpListenerActor.props(this);
    }

    @Override
    protected Parser<List<Record>, ByteBuffer> getParser() {
        return _parser;
    }

    private TelegrafJsonTcpSource(final Builder builder) {
        super(builder);
        _parser = ThreadLocalBuilder.build(
                TelegrafJsonToRecordParser.Builder.class,
                b -> b.setTimestampUnit(builder._timestampUnit));
    }

    private final Parser<List<Record>, ByteBuffer> _parser;

    /**
     * GraphitePlaintextTcpSource {@code Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends TcpLineSource.Builder<Builder, TelegrafJsonTcpSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TelegrafJsonTcpSource::new);
            setPort(DEFAULT_PORT);
            setName(DEFAULT_NAME);
        }

        /**
         * The timestamp unit. Optional. Cannot be null. Default is {@code SECONDS}.
         *
         * @param value the timestamp unit
         * @return this instance of {@link Builder}
         */
        public Builder setTimestampUnit(final TimestampUnit value) {
            this._timestampUnit = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private TimestampUnit _timestampUnit = TimestampUnit.SECONDS;

        private static final int DEFAULT_PORT = 8094;
        private static final String DEFAULT_NAME = "telegraf_json_tcp_source";
    }
}

