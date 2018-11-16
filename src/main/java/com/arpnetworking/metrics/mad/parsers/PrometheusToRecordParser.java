/*
 * Copyright 2018 Inscope Metrics, Inc.
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
import java.util.UUID;

/**
 * Parses the Prometheus protobuf binary protocol into records.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
public class PrometheusToRecordParser implements Parser<List<Record>, HttpRequest> {
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

    private static String createUnitMapKey(final String name) {
        return new StringBuilder(name).reverse().toString();
    }

    Unit parseUnit(final String name) {
        if (name != null) {
            final StringBuilder builder = new StringBuilder();
            for (int i = name.length() - 1; i >= 0; i--) {
                final char ch = name.charAt(i);
                if (ch == '_') {
                    final String key = builder.toString();
                    if (PROMETHEUS_AGGREGATION_KEYS.contains(key)) {
                        builder.setLength(0); //reset builder
                    } else {
                        return UNIT_MAP.get(key);
                    }
                } else {
                    builder.append(ch);
                }
            }
            return UNIT_MAP.get(builder.toString());
        }
        return null;
    }

    @Override
    public List<Record> parse(final HttpRequest data) throws ParsingException {
        final List<Record> records = Lists.newArrayList();
        try {
            final byte[] uncompressed = Snappy.uncompress(data.getBody().toArray());
            final Remote.WriteRequest writeRequest = Remote.WriteRequest.parseFrom(uncompressed);
            for (final TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                String name = null;
                final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
                for (Types.Label label : timeSeries.getLabelsList()) {
                    if (label.getName().equals("__name__")) {
                        name = label.getValue();
                    } else {
                        dimensionsBuilder.put(label.getName(), label.getValue());
                    }
                }
                final ImmutableMap<String, String> immutableDimensions = dimensionsBuilder.build();
                final String metricName = name;
                final Unit unit = parseUnit(name);
                for (final Types.Sample sample : timeSeries.getSamplesList()) {
                    final Record record = ThreadLocalBuilder.build(
                            DefaultRecord.Builder.class,
                            b -> b.setId(UUID.randomUUID().toString())
                                    .setTime(
                                            ZonedDateTime.ofInstant(
                                                    Instant.ofEpochMilli(sample.getTimestamp()),
                                                    ZoneOffset.UTC))
                                    .setMetrics(createMetric(metricName, sample, unit))
                                    .setDimensions(immutableDimensions)
                    );
                    records.add(record);
                }
            }
        } catch (final InvalidProtocolBufferException e) {
            throw new ParsingException("Could not create Request message from data", data.getBody().toArray(), e);
        } catch (final ConstraintsViolatedException | IllegalArgumentException e) {
            throw new ParsingException("Could not build record", data.getBody().toArray(), e);
        } catch (final IOException e) {
            throw new ParsingException("Could not read data", data.getBody().toArray(), e);
        }
        return records;
    }

    private ImmutableMap<String, ? extends Metric> createMetric(final String name, final Types.Sample sample, final Unit unit) {
        final Metric metric = ThreadLocalBuilder.build(
                DefaultMetric.Builder.class,
                p -> p
                        .setType(MetricType.GAUGE)
                        .setValues(ImmutableList.of(createQuantity(sample, unit)))
                        .build()
        );
        return ImmutableMap.of(name, metric);
    }

    private Quantity createQuantity(final Types.Sample sample, final Unit unit) {
        return ThreadLocalBuilder.build(
                Quantity.Builder.class,
                p -> p
                        .setValue(sample.getValue())
                        .setUnit(unit)
        );
    }

}
