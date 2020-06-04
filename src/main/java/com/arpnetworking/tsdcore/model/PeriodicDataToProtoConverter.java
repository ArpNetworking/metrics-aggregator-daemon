/*
 * Copyright 2020 Dropbox Inc.
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class to convert PeriodicDatas to metrics-aggregator-protocol protobuf messages.
 *
 * @author William Ehlhardt (whale at dropbox dot com)
 */
public final class PeriodicDataToProtoConverter {
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic EXPRESSION_STATISTIC = STATISTIC_FACTORY.getStatistic("expression");

    private final Duration _period;
    private final ZonedDateTime _periodStart;
    private final ImmutableMap<String, String> _dimensionParameters;
    private final String _cluster;
    private final String _service;

    /**
     * Create a converter to generate protobuf messages for a given PeriodicData.
     *
     * @param periodicData Originating PeriodicData
     */
    public PeriodicDataToProtoConverter(final PeriodicData periodicData) {
        _period = periodicData.getPeriod();
        _periodStart = periodicData.getStart();
        _dimensionParameters = periodicData.getDimensions().getParameters();
        _cluster = periodicData.getDimensions().getCluster();
        _service = periodicData.getDimensions().getService();
    }

    /**
     * Convert a metric's data to a StatisticSetRecord.
     *
     * @param metricName   Name of metric being converted.
     * @param data         Recorded metric data to serialize.
     * @return StatisticSetRecord protobuf corresponding to the above.
     */
    public Messages.StatisticSetRecord convert(
            final String metricName,
            final Collection<AggregatedData> data) {

        final Messages.StatisticSetRecord.Builder builder = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(_period.toString())
                .setPeriodStart(_periodStart.toString())
                .putAllDimensions(_dimensionParameters)
                .setCluster(_cluster)
                .setService(_service);

        for (final AggregatedData datum : data) {
            if (Objects.equals(EXPRESSION_STATISTIC, datum.getStatistic())) {
                continue;
            }

            final String unit;
            if (datum.getValue().getUnit().isPresent()) {
                // TODO(ville): The protocol needs to support compound units.
                unit = datum.getValue().getUnit().get().toString();
            } else {
                unit = "";
            }

            final Messages.StatisticRecord.Builder entryBuilder = builder.addStatisticsBuilder()
                    .setStatistic(datum.getStatistic().getName())
                    .setValue(datum.getValue().getValue())
                    .setUnit(unit)
                    .setUserSpecified(datum.getIsSpecified());

            final ByteString supportingData = serializeSupportingData(datum);
            if (supportingData != null) {
                entryBuilder.setSupportingData(supportingData);
            }
            entryBuilder.build();
        }

        return builder.build();
    }

    private static ByteString serializeSupportingData(final AggregatedData datum) {
        final Object data = datum.getSupportingData();
        final ByteString byteString;
        if (data instanceof HistogramStatistic.HistogramSupportingData) {
            final HistogramStatistic.HistogramSupportingData histogramSupportingData = (HistogramStatistic.HistogramSupportingData) data;
            final Messages.SparseHistogramSupportingData.Builder builder = Messages.SparseHistogramSupportingData.newBuilder();
            final HistogramStatistic.HistogramSnapshot histogram = histogramSupportingData.getHistogramSnapshot();
            final String unit;
            if (histogramSupportingData.getUnit().isPresent()) {
                // TODO(ville): The protocol needs to support compound units.
                unit = histogramSupportingData.getUnit().get().toString();
            } else {
                unit = "";
            }
            builder.setUnit(unit);

            for (final Map.Entry<Double, Long> entry : histogram.getValues()) {
                builder.addEntriesBuilder()
                        .setBucket(entry.getKey())
                        .setCount(entry.getValue())
                        .build();
            }
            byteString = ByteString.copyFrom(
                    AggregationMessage.create(builder.build()).serializeToByteString().toArray());
        } else {
            return null;
        }
        return byteString;
    }
}
