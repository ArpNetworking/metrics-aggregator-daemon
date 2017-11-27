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

import com.arpnetworking.commons.builder.OvalBuilder;
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
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Parses Telegraf JSON data as a {@link Record}. As defined here.
 *
 * https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#json
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TelegrafJsonToRecordParser implements Parser<List<Record>, ByteBuffer> {

    /**
     * Parses a telegraf JSON datagram.
     *
     * @param data a telegraph JSON blob
     * @return A list of {@link DefaultRecord.Builder}
     * @throws ParsingException if the data is not parsable as telegraf JSON
     */
    public List<Record> parse(final ByteBuffer data) throws ParsingException {
        try {
            final Telegraf telegraf = OBJECT_MAPPER.readValue(data.array(), Telegraf.class);
            final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();
            for (final Map.Entry<String, Number> entry : telegraf.getFields().entrySet()) {
                metrics.put(
                        telegraf.getName().isEmpty() ? entry.getKey() : telegraf.getName() + "." + entry.getKey(),
                        new DefaultMetric.Builder()
                                .setType(MetricType.TIMER)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder()
                                                .setValue(entry.getValue().doubleValue())
                                                .build()))
                                .build());
            }
            final DateTime timestamp = _timestampUnit.create(telegraf.getTimestamp());
            return Collections.singletonList(
                    new DefaultRecord.Builder()
                            .setId(UUID_FACTORY.create().toString())
                            .setMetrics(metrics.build())
                            .setDimensions(telegraf.getTags())
                            .setTime(timestamp)
                            .build());
        } catch (final IOException e) {
            throw new ParsingException("Invalid json", data.array(), e);
        }
    }

    private TelegrafJsonToRecordParser(final Builder builder) {
        _timestampUnit = builder._timestampUnit;
    }

    private final TimestampUnit _timestampUnit;

    private static final UuidFactory UUID_FACTORY = new SplittableRandomUuidFactory();
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    /**
     * Implementation of <code>Builder</code> for {@link TelegrafJsonToRecordParser}.
     */
    public static final class Builder extends OvalBuilder<TelegrafJsonToRecordParser> {

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
        public abstract DateTime create(final long timestamp);
    }
}
