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
package com.arpnetworking.tsdcore.sinks;

import akka.util.ByteString;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.metrics.mad.model.statistics.CountStatistic;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.PeriodicDataToProtoConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

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
    protected Collection<SerializedDatum> serialize(final PeriodicData periodicData) {
        final Collection<Messages.StatisticSetRecord> records = PeriodicDataToProtoConverter.convert(periodicData);

        final ImmutableList.Builder<SerializedDatum> serializedPeriodicData = ImmutableList.builder();
        for (final Messages.StatisticSetRecord record : records) {
            serializedPeriodicData.add(new SerializedDatum(
                    AggregationMessage.create(record).serializeToByteString().toArray(),
                    computePopulationSize(record.getMetric(), record.getStatisticsList())
                    )
            );
        }
        return serializedPeriodicData.build();
    }

    private static long computePopulationSize(final String metricName, final List<Messages.StatisticRecord> statisticsList) {
        final ImmutableMap.Builder<Statistic, Long> calculatedValuesBuilder = ImmutableMap.builder();
        for (final Messages.StatisticRecord statisticRecord : statisticsList) {
            final Optional<Statistic> statisticOptional = STATISTIC_FACTORY.tryGetStatistic(statisticRecord.getStatistic());
            if (!statisticOptional.isPresent()) {
                    continue;
                }
            final Statistic statistic = statisticOptional.get();
            if (statistic instanceof HistogramStatistic) {
                final Messages.SparseHistogramSupportingData supportingData = deserialzeSupportingData(statisticRecord);
                final HistogramStatistic.Histogram histogram = new HistogramStatistic.Histogram();
                for (final Messages.SparseHistogramEntry entry : supportingData.getEntriesList()) {
                    final double bucket = entry.getBucket();
                    final long count = entry.getCount();
                    histogram.recordValue(bucket, count);
                }
                calculatedValuesBuilder.put(statistic, histogram.getSnapshot().getEntriesCount());
            } else if (statistic instanceof CountStatistic) {
                calculatedValuesBuilder.put(statistic, (long) statisticRecord.getValue());
            }
        }
       final ImmutableMap<Statistic, Long> calculatedValues = calculatedValuesBuilder.build();

        // Prefer using the histogram since it's value is always accurate
        if (calculatedValues.containsKey(HISTOGRAM_STATISTIC)) {
            return calculatedValues.get(HISTOGRAM_STATISTIC);
        }

        if (calculatedValues.containsKey(COUNT_STATISTIC)) {
            return calculatedValues.get(COUNT_STATISTIC);
        }

        NO_POPULATION_SIZE_LOGGER.warn()
                .setMessage("Unable to compute population size")
                .addData("metric", metricName)
                .log();
        return 1L;
    }

    @SuppressWarnings("unchecked")
    private static <T> T deserialzeSupportingData(final Messages.StatisticRecord record) {
        if (record.getSupportingData() == null) {
            throw new RuntimeException(new IllegalArgumentException("no supporting data found"));
        }
        return (T) AggregationMessage.deserialize(
                ByteString.fromByteBuffer(record.getSupportingData().asReadOnlyByteBuffer())).get().getMessage();
    }

    private AggregationServerHttpSink(final Builder builder) {
        super(builder);
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");
    private static final Statistic HISTOGRAM_STATISTIC = STATISTIC_FACTORY.getStatistic("histogram");
    private static final Logger NO_POPULATION_SIZE_LOGGER =
            LoggerFactory.getRateLimitLogger(AggregationServerHttpSink.class, java.time.Duration.ofSeconds(30));

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
