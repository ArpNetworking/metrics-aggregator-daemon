/**
 * Copyright 2017 Inscope Metrics, Inc.
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
package com.arpnetworking.metrics.mad.parsers;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.commons.uuidfactory.SplittableRandomUuidFactory;
import com.arpnetworking.commons.uuidfactory.UuidFactory;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.json.Telegraf;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Parses Telegraf JSON data as a {@link Record}. As defined here:
 *
 * https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#json
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
 *     type="com.arpnetworking.metrics.common.sources.TcpLineSource"
 *     actorName="telegraf-tcp-source"
 *     name="telegraftcp_source"
 *     host="0.0.0.0"
 *     port="8094"
 *     parser={
 *       type="com.arpnetworking.metrics.mad.parsers.TelegrafJsonToRecordParser"
 *       timestampUnit="NANOSECONDS"
 *     }
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
 * json_timestamp_units = "1ns"
 * keep_alive_period = "5m"
 *
 * [[inputs.cpu]]
 * percpu = true
 * totalcpu = true
 * collect_cpu_time = false
 * report_active = false
 * </pre>
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TelegrafJsonToRecordParser implements Parser<List<Record>, ByteBuffer> {

    /**
     * Parses a telegraf JSON record.
     *
     * @param record a telegraph JSON record
     * @return A list of {@link DefaultRecord.Builder}
     * @throws ParsingException if the data is not parsable as telegraf JSON
     */
    public List<Record> parse(final ByteBuffer record) throws ParsingException {
        try {
            final Telegraf telegraf = OBJECT_MAPPER.readValue(record.array(), Telegraf.class);
            final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();
            for (final Map.Entry<String, String> entry : telegraf.getFields().entrySet()) {
                final @Nullable Double value = parseValue(entry.getValue());
                if (value != null) {
                    metrics.put(
                            telegraf.getName().isEmpty() ? entry.getKey() : telegraf.getName() + "." + entry.getKey(),
                            ThreadLocalBuilder.build(
                                    DefaultMetric.Builder.class,
                                    b1 -> b1.setType(MetricType.TIMER)
                                    .setValues(ImmutableList.of(
                                            ThreadLocalBuilder.build(
                                                    Quantity.Builder.class,
                                                    b2 -> b2.setValue(value))))));
                }
            }
            final DateTime timestamp = _timestampUnit.create(telegraf.getTimestamp());
            return Collections.singletonList(
                    ThreadLocalBuilder.build(
                            DefaultRecord.Builder.class,
                            b -> b.setId(UUID_FACTORY.create().toString())
                                    .setMetrics(metrics.build())
                                    .setDimensions(telegraf.getTags())
                                    .setTime(timestamp)));
        } catch (final IOException e) {
            throw new ParsingException("Invalid json", record.array(), e);
        }
    }

    private @Nullable Double parseValue(final String value) {
        try {
            return NUMBER_FORMAT.get().parse(value).doubleValue();
        } catch (final ParseException e) {
            if ("true".equalsIgnoreCase(value)) {
                return 1d;
            } else if ("false".equalsIgnoreCase(value)) {
                return 0d;
            }
        }
        return null;
    }

    private TelegrafJsonToRecordParser(final Builder builder) {
        _timestampUnit = builder._timestampUnit;
    }

    private final TimestampUnit _timestampUnit;

    private static final UuidFactory UUID_FACTORY = new SplittableRandomUuidFactory();
    private static final ThreadLocal<NumberFormat> NUMBER_FORMAT = ThreadLocal.withInitial(NumberFormat::getInstance);
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    /**
     * Implementation of <code>Builder</code> for {@link TelegrafJsonToRecordParser}.
     */
    public static final class Builder extends ThreadLocalBuilder<TelegrafJsonToRecordParser> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TelegrafJsonToRecordParser::new);
        }

        /**
         * The timestamp unit. Optional. Cannot be null if set. Default is seconds.
         *
         * @param value The timestamp unit.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setTimestampUnit(final TimestampUnit value) {
            this._timestampUnit = value;
            return this;
        }

        @Override
        protected void reset() {
            _timestampUnit = null;
        }

        @NotNull
        private TimestampUnit _timestampUnit = TimestampUnit.SECONDS;
    }

    /**
     * Timestamp units for Telegraf JSON data.
     */
    public enum TimestampUnit {
        /**
         * Telegraf JSON timestamp in seconds.
         */
        SECONDS {
            @Override
            public DateTime create(final long timestamp) {
                return new DateTime(timestamp * 1000, DateTimeZone.UTC);
            }
        },
        /**
         * Telegraf JSON timestamp in milliseconds.
         */
        MILLISECONDS {
            @Override
            public DateTime create(final long timestamp) {
                return new DateTime(timestamp, DateTimeZone.UTC);
            }
        },
        /**
         * Telegraf JSON timestamp in microseconds.
         */
        MICROSECONDS {
            @Override
            public DateTime create(final long timestamp) {
                return new DateTime(timestamp / 1000, DateTimeZone.UTC);
            }
        },
        /**
         * Telegraf JSON timestamp in nanoseconds.
         */
        NANOSECONDS {
            @Override
            public DateTime create(final long timestamp) {
                return new DateTime(timestamp / 1000000, DateTimeZone.UTC);
            }
        };

        /**
         * Convert a {@code long} epoch in this unit into a {@code DateTime}.
         *
         * @param timestamp the {@code long} epoch in this unit
         * @return instance of {@code DateTime}
         */
        public abstract DateTime create(long timestamp);
    }
}
