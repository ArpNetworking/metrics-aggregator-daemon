/*
 * Copyright 2018 Bruno Green.
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
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import com.arpnetworking.metrics.prometheus.Types.TimeSeries;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import net.sf.oval.exception.ConstraintsViolatedException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Parses the Prometheus protobuf binary protocol into records.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
public class PrometheusToRecordParser implements Parser<List<Record>, HttpRequest> {
    /**
     * public constructor.
     * @param interpretUnits specifies whether or not to interpret units.
     */
    public PrometheusToRecordParser(final boolean interpretUnits) {
        _interpretUnits = interpretUnits;
    }
    /**
     * Parses a unit from the name of a metric.
     * Prometheus will, by default, add unit names to the end of a metric name.
     * We want to parse that name and apply that unit to the metric.
     * An unit suffix might be added to the name of the metric, we currently have a set of
     * whitelisted suffixes that is most likely not exhaustive.
     * For more information see: https://prometheus.io/docs/practices/naming/
     * */
    Optional<ParseResult> parseUnit(final String name) {
        if (!_interpretUnits) {
            return Optional.empty();
        }
        final StringBuilder builder = new StringBuilder();
        for (int i = name.length() - 1; i >= 0; i--) {
            final char ch = name.charAt(i);
            if (ch == '_') {
                final String key = builder.toString();
                if (PROMETHEUS_AGGREGATION_KEYS.contains(key)) {
                    builder.setLength(0); //reset builder
                } else {
                    final Unit value = UNIT_MAP.get(key);
                    if (value != null) {
                        final String newName = name.substring(0, i).concat(name.substring(i + 1 + key.length()));
                        return Optional.of(new ParseResult(value, newName));
                    } else {
                        return Optional.empty();
                    }
                }
            } else {
                builder.append(ch);
            }
        }
        final String possibleUnit = builder.toString();
        final Unit value = UNIT_MAP.get(possibleUnit);
        if (value != null) {
            final String newName = name.substring(Math.min(possibleUnit.length() + 1, name.length()));
            return Optional.of(new ParseResult(value, newName));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Record> parse(final HttpRequest data) throws ParsingException {
        final List<Record> records = Lists.newArrayList();
        final byte[] uncompressed;
        try {
            uncompressed = Snappy.uncompress(data.getBody().toArray());
        } catch (final IOException e) {
            throw new ParsingException("Failed to decompress snappy stream", data.getBody().toArray(), e);
        }
        try {
            final Remote.WriteRequest writeRequest = Remote.WriteRequest.parseFrom(uncompressed);
            for (final TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                Optional<String> nameOpt = Optional.empty();
                final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
                for (Types.Label label : timeSeries.getLabelsList()) {
                    if ("__name__".equals(label.getName())) {
                        final String value = label.getValue();
                        nameOpt = Optional.ofNullable(value);
                    } else {
                        dimensionsBuilder.put(label.getName(), label.getValue());
                    }
                }
                if (!nameOpt.isPresent()) {
                    throw new ParsingException("Could not find the metric name", data.getBody().toArray());
                }
                final String metricName = nameOpt.get().trim();
                if (metricName.isEmpty()) {
                    throw new ParsingException("Found a metric with an empty name", data.getBody().toArray());
                }
                final Optional<ParseResult> result = parseUnit(metricName);
                final ImmutableMap<String, String> immutableDimensions = dimensionsBuilder.build();
                for (final Types.Sample sample : timeSeries.getSamplesList()) {
                    final Record record = ThreadLocalBuilder.build(
                            DefaultRecord.Builder.class,
                            b -> b.setId(UUID.randomUUID().toString())
                                    .setTime(
                                            ZonedDateTime.ofInstant(
                                                    Instant.ofEpochMilli(sample.getTimestamp()),
                                                    ZoneOffset.UTC))
                                    .setMetrics(
                                            createMetric(
                                                    result.map(ParseResult::getNewName).orElse(metricName),
                                                    sample,
                                                    result.map(ParseResult::getUnit)))
                                    .setDimensions(immutableDimensions)
                    );
                    records.add(record);
                }
            }
        } catch (final InvalidProtocolBufferException e) {
            throw new ParsingException("Could not create Request message from data", data.getBody().toArray(), e);
        } catch (final ConstraintsViolatedException | IllegalArgumentException e) {
            throw new ParsingException("Could not build record", data.getBody().toArray(), e);
        }
        return records;
    }

    private ImmutableMap<String, ? extends Metric> createMetric(final String name, final Types.Sample sample, final Optional<Unit> unit) {
        final Metric metric = ThreadLocalBuilder.build(
                DefaultMetric.Builder.class,
                p -> p
                        .setType(MetricType.GAUGE)
                        .setValues(ImmutableList.of(createQuantity(sample, unit)))
                        .build()
        );
        return ImmutableMap.of(name, metric);
    }

    private Quantity createQuantity(final Types.Sample sample, final Optional<Unit> unit) {
        return ThreadLocalBuilder.build(
                Quantity.Builder.class,
                p -> p
                        .setValue(sample.getValue())
                        .setUnit(unit.orElse(null))
        );
    }

    private static String createUnitMapKey(final String name) {
        return new StringBuilder(name).reverse().toString();
    }

    private final boolean _interpretUnits;

    private static final ImmutableMap<String, Unit> UNIT_MAP = ImmutableMap.of(
            createUnitMapKey("seconds"), Unit.SECOND,
            createUnitMapKey("celcius"), Unit.CELCIUS,
            createUnitMapKey("bytes"), Unit.BYTE,
            createUnitMapKey("bits"), Unit.BIT
    );
    private static final ImmutableSet<String> PROMETHEUS_AGGREGATION_KEYS = ImmutableSet.of(
            createUnitMapKey("total"),
            createUnitMapKey("bucket"),
            createUnitMapKey("sum"),
            createUnitMapKey("avg"),
            createUnitMapKey("count")
    );

    static class ParseResult{
        private final Unit _unit;

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ParseResult that = (ParseResult) o;
            return _unit == that._unit
                    && _newName.equals(that._newName);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("unit", _unit)
                    .add("newName", _newName)
                    .toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(_unit, _newName);
        }

        private final String _newName;
        ParseResult(final Unit unit, final String newName) {
            _unit = unit;
            _newName = newName;
        }

        public Unit getUnit() {
            return _unit;
        }

        public String getNewName() {
            return _newName;
        }
    }
}
