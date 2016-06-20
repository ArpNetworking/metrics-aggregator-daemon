/**
 * Copyright 2014 Brandon Arp
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.statistics.HistogramStatistic;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.joda.time.DateTime;
import org.vertx.java.core.Handler;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Publisher to send data to an upstream aggregation server.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public final class AggregationServerSink extends VertxSink {
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", periodicData.getData().size())
                .addData("conditionsSize", periodicData.getConditions().size())
                .log();

        final Map<String, List<AggregatedData>> data = periodicData.getData()
                .stream()
                .filter(datum -> !EXPRESSION_STATISTIC.equals(datum.getFQDSN().getStatistic()))
                .collect(Collectors.groupingBy(d -> d.getFQDSN().getMetric()));
        for (final List<AggregatedData> dataList : data.values()) {
            if (!dataList.isEmpty()) {
                final Messages.StatisticSetRecord record = serializeMetricData(periodicData, dataList);
                enqueueData(AggregationMessage.create(record).serialize());
            }
        }
    }

    private Messages.StatisticSetRecord serializeMetricData(final PeriodicData periodicData, final List<AggregatedData> dataList) {
        // Get the cluster from the first data element
        final AggregatedData firstElement = dataList.iterator().next();

        final List<Messages.DimensionEntry> dimensions = Lists.newArrayList();
        for (final Map.Entry<String, String> entry : periodicData.getDimensions().entrySet()) {
            dimensions.add(
                    Messages.DimensionEntry.newBuilder()
                            .setKey(entry.getKey())
                            .setValue(entry.getValue())
                            .build()
            );
        }

        final Messages.StatisticSetRecord.Builder builder = Messages.StatisticSetRecord.newBuilder()
                .setMetric(firstElement.getFQDSN().getMetric())
                .setPeriod(periodicData.getPeriod().toString())
                .setPeriodStart(periodicData.getStart().toString())
                .addAllDimensions(dimensions)
                .setCluster(firstElement.getFQDSN().getCluster())
                .setService(firstElement.getFQDSN().getService());

        for (final AggregatedData datum : dataList) {
            if (EXPRESSION_STATISTIC.equals(datum.getFQDSN().getStatistic())) {
                continue;
            }

            final String unit;
            if (datum.getValue().getUnit().isPresent()) {
                unit = datum.getValue().getUnit().get().toString();
            } else {
                unit = "";
            }

            final Messages.StatisticRecord.Builder entryBuilder = builder.addStatisticsBuilder()
                    .setStatistic(datum.getFQDSN().getStatistic().getName())
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
            byteString = ByteString.copyFrom(AggregationMessage.create(builder.build()).serialize().getBytes());
        } else {
            return null;
        }
        return byteString;
    }

    private void heartbeat() {

        final Messages.HeartbeatRecord message = Messages.HeartbeatRecord.newBuilder()
                .setTimestamp(DateTime.now().toString())
                .build();
        sendRawData(AggregationMessage.create(message).serialize());
        LOGGER.debug()
                .setMessage("Heartbeat sent to aggregation server")
                .addData("sink", getName())
                .log();
    }

    private AggregationServerSink(final Builder builder) {
        super(builder);
        super.getVertx().setPeriodic(15000, new Handler<Long>() {
            @Override
            public void handle(final Long event) {
                LOGGER.trace()
                        .setMessage("Heartbeat tick")
                        .addData("sink", getName())
                        .log();
                heartbeat();
            }
        });
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic EXPRESSION_STATISTIC = STATISTIC_FACTORY.getStatistic("expression");
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationServerSink.class);

    /**
     * Implementation of builder pattern for <code>AggreationServerSink</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends VertxSink.Builder<Builder, AggregationServerSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregationServerSink::new);
            setServerPort(7065);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }
    }
}
