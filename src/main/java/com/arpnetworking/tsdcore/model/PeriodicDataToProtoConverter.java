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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Helper class to convert PeriodicDatas to metrics-aggregator-protocol protobuf messages.
 *
 * @author William Ehlhardt (whale at dropbox dot com)
 */
public final class PeriodicDataToProtoConverter {
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic EXPRESSION_STATISTIC = STATISTIC_FACTORY.getStatistic("expression");
    private static final Statistic HISTOGRAM_STATISTIC = STATISTIC_FACTORY.getStatistic("histogram");
    private static final Statistic COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");

    /**
     * Convert a PeriodicData to a set of {@link ConvertedDatum}.
     *
     * @param periodicData PeriodicData being converted.
     * @return List of {@link ConvertedDatum} corresponding to the above.
     */
    public static List<ConvertedDatum> convert(
            final PeriodicData periodicData
    ) {
        final ImmutableList.Builder<ConvertedDatum> convertedData = ImmutableList.builder();
        for (final Map.Entry<String, Collection<AggregatedData>> entry : periodicData.getData().asMap().entrySet()) {
            final String metricName = entry.getKey();
            final Collection<AggregatedData> data = entry.getValue();
            if (!data.isEmpty()) {
                final ConvertedDatum convertedDatum = convertAggregatedData(
                        periodicData, metricName, data);
                convertedData.add(convertedDatum);
            }
        }
        return convertedData.build();

    }

    private static ConvertedDatum convertAggregatedData(
            final PeriodicData periodicData,
            final String metricName,
            final Collection<AggregatedData> data) {
        final Messages.StatisticSetRecord.Builder builder = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(periodicData.getPeriod().toString())
                .setPeriodStart(periodicData.getStart().toString())
                .setClientMinimumRequestTime(periodicData.getMinRequestTime().map(ZonedDateTime::toString).orElse(""))
                .putAllDimensions(periodicData.getDimensions().getParameters())
                .setCluster(periodicData.getDimensions().getCluster())
                .setService(periodicData.getDimensions().getService());

        final ImmutableMap.Builder<Statistic, Long> populationSizesBuilder = ImmutableMap.builder();
        for (final AggregatedData datum : data) {
            final Statistic statistic = datum.getStatistic();
            if (Objects.equals(EXPRESSION_STATISTIC, statistic)) {
                continue;
            }

            if (Objects.equals(HISTOGRAM_STATISTIC, statistic) || Objects.equals(COUNT_STATISTIC, statistic)) {
                populationSizesBuilder.put(statistic, datum.getPopulationSize());
            }

            final String unit;
            if (datum.getValue().getUnit().isPresent()) {
                // TODO(ville): The protocol needs to support compound units.
                unit = datum.getValue().getUnit().get().toString();
            } else {
                unit = "";
            }

            final Messages.StatisticRecord.Builder entryBuilder = builder.addStatisticsBuilder()
                    .setStatistic(statistic.getName())
                    .setValue(datum.getValue().getValue())
                    .setUnit(unit)
                    .setUserSpecified(datum.getIsSpecified());

            final ByteString supportingData = serializeSupportingData(datum);
            if (supportingData != null) {
                entryBuilder.setSupportingData(supportingData);
            }
            entryBuilder.build();
        }

        final ImmutableMap<Statistic, Long> populationSizes = populationSizesBuilder.build();

        final long populationSize;
        if (populationSizes.containsKey(HISTOGRAM_STATISTIC)) {
            final Long boxed = populationSizes.get(HISTOGRAM_STATISTIC);
            populationSize = boxed == null ? 0 : boxed;
        } else {
            final Long boxed = populationSizes.getOrDefault(COUNT_STATISTIC, 1L);
            populationSize = boxed == null ? 1 : boxed;
        }

        return new ConvertedDatum(builder.build(), populationSize);


    }

    @Nullable
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

            // Need to add 1 since 0 is the default value and indicates no precision was set.
            builder.setPrecision(histogram.getPrecision() + 1);

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

    private PeriodicDataToProtoConverter() {}

    /**
     * Contains the StatisticSetRecord protobuf and its corresponding population size.
     *
     */
    public static final class ConvertedDatum{
        ConvertedDatum(final Messages.StatisticSetRecord record, final long populationSize) {
            _record = record;
            _populationSize = populationSize;
        }

        public Messages.StatisticSetRecord getStatisticSetRecord() {
            return _record;
        }

        public long getPopulationSize() {
            return _populationSize;
        }

        private final Messages.StatisticSetRecord _record;
        private final long _populationSize;
    }
}
