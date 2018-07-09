/*
 * Copyright 2018 Dropbox Inc.
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
package com.inscopemetrics.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import com.google.protobuf.ByteString;
import com.inscopemetrics.tsdcore.model.AggregatedData;
import com.inscopemetrics.tsdcore.model.AggregationMessage;
import com.inscopemetrics.tsdcore.model.PeriodicData;
import com.inscopemetrics.tsdcore.statistics.HistogramStatistic;
import com.inscopemetrics.tsdcore.statistics.Statistic;
import com.inscopemetrics.tsdcore.statistics.StatisticFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Publisher to send data to an upstream aggregation server over HTTP.
 *
 * @author Ville Koskela (vkoskela at dropbox dot com)
 */
public final class AggregationServerHttpSink extends HttpPostSink {

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .build();
    }

    @Override
    protected String getContentType() {
        return MediaType.PROTOBUF.toString();
    }

    @Override
    protected Collection<byte[]> serialize(final PeriodicData periodicData) {
        final ImmutableList.Builder<byte[]> serializedPeriodicData = ImmutableList.builder();
        for (final Map.Entry<String, Collection<AggregatedData>> entry : periodicData.getData().asMap().entrySet()) {
            final String metricName = entry.getKey();
            final Collection<AggregatedData> data = entry.getValue();
            if (!data.isEmpty()) {
                final Messages.StatisticSetRecord record = serializeMetricData(periodicData, metricName, data);
                serializedPeriodicData.add(AggregationMessage.create(record).serializeToByteString().toArray());
            }
        }
        return serializedPeriodicData.build();
    }

    private Messages.StatisticSetRecord serializeMetricData(
            final PeriodicData periodicData,
            final String metricName,
            final Collection<AggregatedData> data) {

        // Create a statistic record set
        final Messages.StatisticSetRecord.Builder builder = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(periodicData.getPeriod().toString())
                .setPeriodStart(periodicData.getStart().toString())
                .putAllDimensions(periodicData.getDimensions().getParameters())
                .setCluster(periodicData.getDimensions().getCluster())
                .setService(periodicData.getDimensions().getService());

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
                    .setUserSpecified(datum.isSpecified());

            final ByteString supportingData = serializeSupportingData(datum);
            if (supportingData != null) {
                entryBuilder.setSupportingData(supportingData);
            }
            entryBuilder.build();
        }

        return builder.build();
    }

    private ByteString serializeSupportingData(final AggregatedData datum) {
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

            for (final Map.Entry<Double, Integer> entry : histogram.getValues()) {
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

    private AggregationServerHttpSink(final Builder builder) {
        super(builder);
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic EXPRESSION_STATISTIC = STATISTIC_FACTORY.getStatistic("expression");

    /**
     * Implementation of builder pattern for ${code AggregationServerHttpSink}.
     *
     * @author Ville Koskela (ville dot koskela at dropbox dot com)
     */
    public static final class Builder extends HttpPostSink.Builder<Builder, AggregationServerHttpSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregationServerHttpSink::new);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
