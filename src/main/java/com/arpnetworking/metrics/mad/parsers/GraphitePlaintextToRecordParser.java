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
package com.arpnetworking.metrics.mad.parsers;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Record;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotNull;

import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Parses Graphite plaintext data as a {@link Record}.
 *
 * https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol
 *
 * The source also allows you to add dimensions in three ways:
 *
 * 1) Specifying global tags programmatically.
 * 2) Parsing tags from the metric name in Carbon format.
 *     See: http://graphite.readthedocs.io/en/latest/tags.html
 * 3) Parsing tags from the metric name in InfluxDb format.
 *     See: https://github.com/influxdata/influxdb/issues/2996
 *     (this is not yet available)
 *
 * Sample MAD configuration:
 * <pre>
 * {
 *   type="com.arpnetworking.metrics.mad.sources.MappingSource"
 *   name="graphitetcp_mapping_source"
 *   actorName="graphite-tcp-source"
 *   findAndReplace={
 *     "\\."=["/"]
 *   }
 *   source={
 *     type="com.arpnetworking.metrics.common.sources.TcpLineSource"
 *     name="graphitetcp_source"
 *     host="0.0.0.0"
 *     port="2003"
 *   }
 * }
 * </pre>
 *
 * TODO(ville): Add pickle support.
 * See:
 * - https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-pickle-protocol
 * - https://github.com/irmen/Pyrolite
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class GraphitePlaintextToRecordParser implements Parser<List<Record>, ByteBuffer> {

    /**
     * Parses a graphite plaintext record.
     *
     * @param record a graphite plaintext record
     * @return A list of {@link DefaultRecord.Builder}
     * @throws ParsingException if the datagram is not parsable as graphite plaintext formatted message
     */
    public List<Record> parse(final ByteBuffer record) throws ParsingException {
        // CHECKSTYLE.OFF: IllegalInstantiation - This is the recommended way
        final String line = new String(record.array(), Charsets.UTF_8);
        // CHECKSTYLE.ON: IllegalInstantiation

        final ImmutableList.Builder<Record> recordListBuilder = ImmutableList.builder();
        final ZonedDateTime now = ZonedDateTime.now();
        final Matcher matcher = GRAPHITE_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new ParsingException("Invalid graphite line", line.getBytes(Charsets.UTF_8));
        }

        // Annotations
        final Map<String, String> dimensions = Maps.newHashMap();

        // Add global dimensions (lowest priority)
        // IMPORTANT: Subsequently processed tags can overwrite these
        dimensions.putAll(_globalTags);

        // Parse the name
        String name = parseName(record, matcher.group("NAME"));
        if (_parseCarbonTags) {
            // IMPORTANT: Subsequently processed tags can overwrite these
            final String[] parts = name.split(";");
            name = parts[0];
            for (int i = 1; i < parts.length; ++i) {
                final String[] subparts = parts[i].split("=");
                if (subparts.length == 2 && !subparts[0].isEmpty() && !subparts[1].isEmpty()) {
                    dimensions.put(subparts[0], subparts[1]);
                }
                // TODO(ville): should we rate limit log that we potentially dropped something?
                // ^ This is a bigger question of how to handle partial invalid data as most
                // parsers now deal with multi-line (or multi-record) input. Currently, partial
                // invalid input fails the entire ByteBuffer. Is that right?
            }
        }

        // Parse the value
        final Number value = parseValue(record, matcher.group("VALUE"));

        // Parse the timestamp
        final ZonedDateTime timestamp = parseTimestamp(record, matcher.group("TIMESTAMP"), now);

        recordListBuilder.add(createRecord(name, value, timestamp, ImmutableMap.copyOf(dimensions)));

        return recordListBuilder.build();
    }

    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    // See: https://github.com/findbugsproject/findbugs/issues/79
    private String parseName(final ByteBuffer datagram, @Nullable final String name) throws ParsingException {
        if (Strings.isNullOrEmpty(name)) {
            throw new ParsingException("Name not found or empty", datagram.array());
        }
        return name;
    }

    private Number parseValue(
            final ByteBuffer datagram,
            @Nullable final String valueAsString)
            throws ParsingException {
        try {
            if (valueAsString == null) {
                throw new ParsingException("Value not found or empty", datagram.array());
            } else {
                return NUMBER_FORMAT.get().parse(valueAsString);
            }
        } catch (final ParseException e) {
            throw new ParsingException("Value is not a number", datagram.array(), e);
        }
    }

    private ZonedDateTime parseTimestamp(
            final ByteBuffer datagram,
            @Nullable final String timestampAsString,
            final ZonedDateTime now)
            throws ParsingException {
        if (null != timestampAsString) {
            try {
                return ZonedDateTime.ofInstant(
                        Instant.ofEpochMilli(NUMBER_FORMAT.get().parse(timestampAsString).longValue() * 1000L),
                        ZoneOffset.UTC);
            } catch (final ParseException e) {
                throw new ParsingException("Timestamp is not a number", datagram.array(), e);
            }
        }
        return now;
    }

    private Record createRecord(
            final String name,
            final Number value,
            final ZonedDateTime timestamp,
            final ImmutableMap<String, String> dimensions) {
        return ThreadLocalBuilder.build(
                DefaultRecord.Builder.class,
                b1 -> b1.setId(UUID.randomUUID().toString())
                .setDimensions(dimensions)
                .setMetrics(ImmutableMap.of(
                        name,
                        ThreadLocalBuilder.build(
                                DefaultMetric.Builder.class,
                                b2 -> b2.setValues(
                                        ImmutableList.of(
                                                ThreadLocalBuilder.build(
                                                        DefaultQuantity.Builder.class,
                                                        b3 -> b3.setValue(value.doubleValue()))))
                                .setType(MetricType.GAUGE))))
                .setTime(timestamp));
    }

    private GraphitePlaintextToRecordParser(final Builder builder) {
        _globalTags = builder._globalTags;
        _parseCarbonTags = builder._parseCarbonTags;
    }

    private final ImmutableMap<String, String> _globalTags;
    private final boolean _parseCarbonTags;

    private static final ThreadLocal<NumberFormat> NUMBER_FORMAT = ThreadLocal.withInitial(NumberFormat::getInstance);
    private static final Pattern GRAPHITE_PATTERN = Pattern.compile(
            "^(?<NAME>[^ ]+) (?<VALUE>[^ ]+)( (?<TIMESTAMP>[0-9]+))?[\\s]*$");

    /**
     * Implementation of <code>Builder</code> for {@link GraphitePlaintextToRecordParser}.
     */
    public static final class Builder extends ThreadLocalBuilder<GraphitePlaintextToRecordParser> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(GraphitePlaintextToRecordParser::new);
        }

        /**
         * Set global tags. Optional. Cannot be null. Default is an empty map.
         *
         * @param value the global tags
         * @return this {@link Builder} instance
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
         * @return this {@link Builder} instance
         */
        public Builder setParseCarbonTags(final Boolean value) {
            _parseCarbonTags = value;
            return this;
        }

        @Override
        protected void reset() {
            _globalTags = ImmutableMap.of();
            _parseCarbonTags = false;
        }

        @NotNull
        private ImmutableMap<String, String> _globalTags = ImmutableMap.of();
        @NotNull
        private Boolean _parseCarbonTags = false;
    }
}
