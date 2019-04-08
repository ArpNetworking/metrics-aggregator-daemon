/*
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
package io.inscopemetrics.mad.parsers;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.commons.uuidfactory.SplittableRandomUuidFactory;
import com.arpnetworking.commons.uuidfactory.UuidFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.inscopemetrics.mad.model.DefaultMetric;
import io.inscopemetrics.mad.model.DefaultRecord;
import io.inscopemetrics.mad.model.Metric;
import io.inscopemetrics.mad.model.MetricType;
import io.inscopemetrics.mad.model.Quantity;
import io.inscopemetrics.mad.model.Record;
import io.inscopemetrics.mad.model.telegraf.TelegrafJson;
import io.inscopemetrics.mad.model.telegraf.TimestampUnit;
import io.inscopemetrics.mad.parsers.exceptions.ParsingException;
import net.sf.oval.constraint.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Parses TelegrafJson JSON data as a {@link Record}. As defined here:
 * <p>
 * https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#json
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
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
            // Parse into abstract json node structure to determine between
            // batch and single metric telegraf json formats.
            final JsonNode jsonNode;
            try {
                jsonNode = OBJECT_MAPPER.readTree(record.array());
            } catch (final IOException e) {
                throw new ParsingException("Invalid json", record.array(), e);
            }

            final ImmutableList<TelegrafJson> telegrafJsonList;
            if (jsonNode.has(METRICS_JSON_KEY)) {
                // Convoluted; see: https://github.com/FasterXML/jackson-databind/issues/1294
                telegrafJsonList = OBJECT_MAPPER.readValue(
                        OBJECT_MAPPER.treeAsTokens(jsonNode.get(METRICS_JSON_KEY)),
                        OBJECT_MAPPER.getTypeFactory().constructType(TELEGRAF_LIST_TYPE_REFERENCE));
            } else {
                final TelegrafJson telegrafJson = OBJECT_MAPPER.treeToValue(jsonNode, TelegrafJson.class);
                telegrafJsonList = ImmutableList.of(telegrafJson);
            }

            final ImmutableList.Builder<Record> records = ImmutableList.builder();
            for (final TelegrafJson telegrafJson : telegrafJsonList) {
                final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();
                for (final Map.Entry<String, String> entry : telegrafJson.getFields().entrySet()) {
                    @Nullable final Double value = parseValue(entry.getValue());
                    if (value != null) {
                        metrics.put(
                                telegrafJson.getName().isEmpty() ? entry.getKey() : telegrafJson.getName() + "." + entry.getKey(),
                                ThreadLocalBuilder.build(
                                        DefaultMetric.Builder.class,
                                        b1 -> b1.setType(MetricType.TIMER)
                                                .setValues(ImmutableList.of(
                                                        ThreadLocalBuilder.build(
                                                                Quantity.Builder.class,
                                                                b2 -> b2.setValue(value))))));
                    }
                }
                final ZonedDateTime timestamp = _timestampUnit.create(telegrafJson.getTimestamp());
                records.add(
                        ThreadLocalBuilder.build(
                                DefaultRecord.Builder.class,
                                b -> b.setId(UUID_FACTORY.create().toString())
                                        .setMetrics(metrics.build())
                                        .setDimensions(telegrafJson.getTags())
                                        .setTime(timestamp)));
            }
            return records.build();
        } catch (final IOException e) {
            throw new ParsingException("Invalid json", record.array(), e);
        }
    }

    private @Nullable
    Double parseValue(final String value) {
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
    private static final TypeReference<ImmutableList<TelegrafJson>> TELEGRAF_LIST_TYPE_REFERENCE =
            new TypeReference<ImmutableList<TelegrafJson>>() {};
    private static final String METRICS_JSON_KEY = "metrics";

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
}
