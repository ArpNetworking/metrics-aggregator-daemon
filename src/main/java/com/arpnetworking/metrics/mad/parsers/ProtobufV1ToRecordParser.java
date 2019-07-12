/*
 * Copyright 2016 Inscope Metrics, Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import net.sf.oval.exception.ConstraintsViolatedException;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Parses the Inscope Metrics protobuf binary protocol into records.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@SuppressWarnings("deprecation")
public final class ProtobufV1ToRecordParser implements Parser<List<Record>, HttpRequest> {
    @Override
    public List<Record> parse(final HttpRequest data) throws ParsingException {
        try {
            final com.inscopemetrics.client.protocol.ClientV1.RecordSet request = 
                    com.inscopemetrics.client.protocol.ClientV1.RecordSet.parseFrom(data.getBody().asByteBuffer());
            final List<Record> records = Lists.newArrayList();
            for (final com.inscopemetrics.client.protocol.ClientV1.Record record : request.getRecordsList()) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(record.getId().toByteArray());
                final Long high = byteBuffer.getLong();
                final Long low = byteBuffer.getLong();
                final Record defaultRecord = ThreadLocalBuilder.build(
                        DefaultRecord.Builder.class,
                        b -> b.setId(new UUID(high, low).toString())
                                .setTime(
                                        ZonedDateTime.ofInstant(
                                                Instant.ofEpochMilli(record.getEndMillisSinceEpoch()),
                                                ZoneOffset.UTC))
                                .setAnnotations(buildAnnotations(record))
                                .setDimensions(buildDimensions(record))
                                .setMetrics(buildMetrics(record)));

                records.add(defaultRecord);
            }
            return records;
        } catch (final InvalidProtocolBufferException e) {
            throw new ParsingException("Could not create Request message from data", data.getBody().toArray(), e);
        } catch (final ConstraintsViolatedException | IllegalArgumentException e) {
            throw new ParsingException("Could not build record", data.getBody().toArray(), e);
        }
    }

    private ImmutableMap<String, ? extends Metric> buildMetrics(
            final com.inscopemetrics.client.protocol.ClientV1.Record record) {
        final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();
        processEntries(metrics, record.getCountersList(), MetricType.COUNTER);
        processEntries(metrics, record.getTimersList(), MetricType.TIMER);
        processEntries(metrics, record.getGaugesList(), MetricType.GAUGE);
        return metrics.build();
    }

    private void processEntries(
            final ImmutableMap.Builder<String, Metric> metrics,
            final List<com.inscopemetrics.client.protocol.ClientV1.MetricEntry> entries,
            final MetricType metricType) {
        for (final com.inscopemetrics.client.protocol.ClientV1.MetricEntry metricEntry : entries) {
            final ImmutableList.Builder<Quantity> quantities =
                    ImmutableList.builderWithExpectedSize(metricEntry.getSamplesCount());
            for (final com.inscopemetrics.client.protocol.ClientV1.DoubleQuantity quantity : metricEntry.getSamplesList()) {
                quantities.add(
                        ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b -> b.setUnit(baseUnit(quantity.getUnit()))
                                        .setValue(quantity.getValue())));
            }

            final Metric defaultMetric = ThreadLocalBuilder.build(
                    DefaultMetric.Builder.class,
                    b -> b.setType(metricType)
                            .setValues(quantities.build()));
            metrics.put(metricEntry.getName(), defaultMetric);
        }
    }

    @Nullable
    private Unit baseUnit(final com.inscopemetrics.client.protocol.ClientV1.CompoundUnit compoundUnit) {
        if (!compoundUnit.getNumeratorList().isEmpty()) {
            final com.inscopemetrics.client.protocol.ClientV1.Unit selectedUnit = compoundUnit.getNumerator(0);
            if (com.inscopemetrics.client.protocol.ClientV1.Unit.Type.UNRECOGNIZED.equals(selectedUnit.getType())) {
                return null;
            }
            final String unitName;
            if (!com.inscopemetrics.client.protocol.ClientV1.Unit.Scale.UNIT.equals(selectedUnit.getScale())
                    && !com.inscopemetrics.client.protocol.ClientV1.Unit.Scale.UNRECOGNIZED.equals(selectedUnit.getScale())) {
                unitName = selectedUnit.getScale().name() + selectedUnit.getType().name();
            } else {
                unitName = selectedUnit.getType().name();
            }

            return Unit.valueOf(unitName);
        }
        return null;
    }

    private ImmutableMap<String, String> buildAnnotations(final com.inscopemetrics.client.protocol.ClientV1.Record record) {
        final ImmutableMap.Builder<String, String> annotations = ImmutableMap.builder();
        for (final com.inscopemetrics.client.protocol.ClientV1.AnnotationEntry annotationEntry : record.getAnnotationsList()) {
            annotations.put(annotationEntry.getName(), annotationEntry.getValue());
        }
        return annotations.build();
    }

    private ImmutableMap<String, String> buildDimensions(final com.inscopemetrics.client.protocol.ClientV1.Record record) {
        final ImmutableMap.Builder<String, String> dimensions = ImmutableMap.builder();
        for (final com.inscopemetrics.client.protocol.ClientV1.DimensionEntry dimensionEntry : record.getDimensionsList()) {
            dimensions.put(dimensionEntry.getName(), dimensionEntry.getValue());
        }
        return dimensions.build();
    }
}
