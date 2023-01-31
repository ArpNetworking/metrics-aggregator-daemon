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
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import com.arpnetworking.metrics.prometheus.Types.TimeSeries;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import net.sf.oval.exception.ConstraintsViolatedException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parses the Prometheus protobuf binary protocol into records.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
public final class PrometheusToRecordParser implements Parser<List<Record>, HttpRequest> {

    /**
     * public constructor.
     *
     * @param interpretUnits specifies whether to interpret units.
     * @param outputDebugInfo specifies whether to output debug files.
     * @param reservedNameWhitelist specifies the whitelist set for reserved names (names with aggregation keys).
     */
    public PrometheusToRecordParser(
            final boolean interpretUnits,
            final boolean outputDebugInfo,
            final HashSet<String> reservedNameWhitelist) {
        _interpretUnits = interpretUnits;
        _outputDebugInfo = outputDebugInfo;
        _reservedNameWhitelist = reservedNameWhitelist;
    }

    /*
     * Parses a unit and the new name from the name of a metric.
     * Prometheus will, by default, add unit names to the end of a metric name.
     * We want to parse that name and apply that unit to the metric.
     * An unit suffix might be added to the name of the metric, we currently have a set of
     * whitelisted suffixes that is most likely not exhaustive.
     * For more information see: https://prometheus.io/docs/practices/naming/
     */
    ParseResult parseNameAndUnit(final String name) {
        Optional<String> aggregationKey = Optional.empty();
        final int lastUnderscore = name.lastIndexOf('_');
        if (lastUnderscore >= 0) {
            final String lastSuffix = name.substring(lastUnderscore + 1);
            if (PROMETHEUS_AGGREGATION_KEYS.contains(lastSuffix)) {
                aggregationKey = Optional.of(lastSuffix);
            }
        }
        if (!_interpretUnits) {
            return new ParseResult(name, aggregationKey, Optional.empty());
        }

        final int unitIndexEnd = aggregationKey.isPresent() ? lastUnderscore - 1 : name.length() - 1;
        final int prevUnderscore = name.lastIndexOf('_', unitIndexEnd);
        final int unitStart;
        if (prevUnderscore >= 0) {
            unitStart = prevUnderscore + 1;
        } else {
            unitStart = 0;
        }
        final String unitString = name.substring(unitStart, unitIndexEnd + 1);
        final Optional<Unit> unit = Optional.ofNullable(UNIT_MAP.get(unitString));
        return new ParseResult(name, aggregationKey, unit);
    }

    @Override
    public List<Record> parse(final HttpRequest data) throws ParsingException {
        final List<Record> records = Lists.newArrayList();
        final byte[] uncompressed = decompress(data);
        try {
            final Remote.WriteRequest writeRequest = Remote.WriteRequest.parseFrom(uncompressed);
            for (final TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                boolean skipSeries = false;
                Optional<String> nameOpt = Optional.empty();
                final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
                for (final Types.Label label : timeSeries.getLabelsList()) {
                    if ("__name__".equals(label.getName())) {
                        final String value = label.getValue();
                        nameOpt = Optional.ofNullable(value);
                    } else if ("le".equals(label.getName())) {
                        skipSeries = true;
                    } else {
                        dimensionsBuilder.put(label.getName(), label.getValue());
                    }
                }
                if (skipSeries) {
                    continue;
                }
                final ParseResult result = parseNameAndUnit(nameOpt.orElse("").trim());
                // We don't currently support aggregate metrics from prometheus
                if (result.getAggregationKey().isPresent() && !_reservedNameWhitelist.contains(result.getName())) {
                    continue;
                }
                final String metricName = result.getName();
                if (metricName.isEmpty()) {
                    throw new ParsingException("Found a metric with an empty name", data.getBody().toArray());
                }
                final ImmutableMap<String, String> immutableDimensions = dimensionsBuilder.build();
                for (final Types.Sample sample : timeSeries.getSamplesList()) {
                    if (!Double.isFinite(sample.getValue())) {
                        continue;
                    }
                    final Record record = ThreadLocalBuilder.build(
                            DefaultRecord.Builder.class,
                            b -> b.setId(UUID.randomUUID().toString())
                                    .setTime(
                                            ZonedDateTime.ofInstant(
                                                    Instant.ofEpochMilli(sample.getTimestamp()),
                                                    ZoneOffset.UTC))
                                    .setMetrics(
                                            createMetric(
                                                    metricName,
                                                    sample,
                                                    result.getUnit()))
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

    private byte[] decompress(final HttpRequest data) throws ParsingException {
        final byte[] uncompressed;
        try {
            final byte[] input = data.getBody().toArray();
            if (_outputDebugInfo) {
                final int outputFile = _outputFileNumber.incrementAndGet();
                if (outputFile < 10) {
                    Files.write(Paths.get("prometheus_debug_" + outputFile), input);
                }
            }
            uncompressed = Snappy.uncompress(input);
        } catch (final IOException e) {
            throw new ParsingException("Failed to decompress snappy stream", data.getBody().toArray(), e);
        }
        return uncompressed;
    }

    private ImmutableMap<String, ? extends Metric> createMetric(final String name, final Types.Sample sample, final Optional<Unit> unit) {
        final Metric metric = ThreadLocalBuilder.build(
                DefaultMetric.Builder.class,
                p -> p
                        .setType(MetricType.GAUGE)
                        .setValues(ImmutableList.of(createQuantity(sample, unit)))
        );
        return ImmutableMap.of(name, metric);
    }

    private Quantity createQuantity(final Types.Sample sample, final Optional<Unit> unit) {
        return ThreadLocalBuilder.build(
                DefaultQuantity.Builder.class,
                p -> p
                        .setValue(sample.getValue())
                        .setUnit(unit.orElse(null))
        );
    }

    private final boolean _interpretUnits;
    private final AtomicInteger _outputFileNumber = new AtomicInteger(0);
    private final boolean _outputDebugInfo;
    private final HashSet<String> _reservedNameWhitelist;

    private static final ImmutableMap<String, Unit> UNIT_MAP = ImmutableMap.of(
            "seconds", Unit.SECOND,
            "celcius", Unit.CELCIUS,
            "bytes", Unit.BYTE,
            "bits", Unit.BIT
    );
    private static final ImmutableSet<String> PROMETHEUS_AGGREGATION_KEYS = ImmutableSet.of(
            "total",
            "bucket",
            "sum",
            "avg",
            "count"
    );

    static final class ParseResult {

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ParseResult that = (ParseResult) o;
            return _unit.equals(that._unit)
                    && _aggregationKey.equals(that._aggregationKey)
                    && _name.equals(that._name);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("unit", _unit)
                    .add("name", _name)
                    .add("aggregationKey", _aggregationKey)
                    .toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(_unit, _name, _aggregationKey);
        }

        public Optional<Unit> getUnit() {
            return _unit;
        }

        public String getName() {
            return _name;
        }

        public Optional<String> getAggregationKey() {
            return _aggregationKey;
        }

        ParseResult(final String name, final Optional<String> aggregationKey, final Optional<Unit> unit) {
            _unit = unit;
            _name = name;
            _aggregationKey = aggregationKey;
        }

        private final String _name;
        private final Optional<String> _aggregationKey;
        private final Optional<Unit> _unit;
    }
}
