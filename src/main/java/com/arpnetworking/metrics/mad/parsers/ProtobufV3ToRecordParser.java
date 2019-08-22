/*
 * Copyright 2019 Dropbox
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
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.inscopemetrics.client.protocol.ClientV3;
import net.sf.oval.exception.ConstraintsViolatedException;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Parses the Inscope Metrics protobuf binary protocol into records.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class ProtobufV3ToRecordParser implements Parser<List<Record>, HttpRequest> {

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();

    @Override
    public List<Record> parse(final HttpRequest data) throws ParsingException {
        try {
            final ClientV3.Request request = ClientV3.Request.parseFrom(data.getBody().asByteBuffer());
            final List<Record> records = Lists.newArrayList();
            for (final ClientV3.Record record : request.getRecordsList()) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(record.getId().toByteArray());
                final long high = byteBuffer.getLong();
                final long low = byteBuffer.getLong();
                records.add(ThreadLocalBuilder.build(
                        DefaultRecord.Builder.class,
                        builder -> builder.setId(new UUID(high, low).toString())
                                .setTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.getEndMillisSinceEpoch()), ZoneOffset.UTC))
                                .setDimensions(buildDimensions(record))
                                .setMetrics(buildMetrics(record))));
            }
            return records;
        } catch (final InvalidProtocolBufferException e) {
            throw new ParsingException("Could not create Request message from data", data.getBody().toArray(), e);
        } catch (final ConstraintsViolatedException | IllegalArgumentException e) {
            throw new ParsingException("Could not build record", data.getBody().toArray(), e);
        }
    }

    private ImmutableMap<String, ? extends Metric> buildMetrics(final ClientV3.Record record) {
        // Merge samples and statistics by metric
        final Map<String, MetricData> metricData = Maps.newHashMap();
        for (final ClientV3.MetricDataEntry metricDataEntry : record.getDataList()) {
            final String metricName = decodeRequiredIdentifier(metricDataEntry.getName());
            switch (metricDataEntry.getDataCase()) {
                case NUMERICALDATA:
                    final ClientV3.NumericalData numericalData = metricDataEntry.getNumericalData();
                    if (!numericalData.getSamplesList().isEmpty()) {
                        parseNumericalSamples(metricName, metricData, numericalData.getSamplesList());
                    }
                    if (numericalData.hasStatistics()) {
                        parseNumericalStatistics(metricName, metricData, numericalData.getStatistics());
                    }
                    break;
                case DATA_NOT_SET:
                    continue;
                default:
                    throw new IllegalArgumentException("Unsupported samples type");
            }
        }

        // Create DefaultMetric instances
        final ImmutableMap.Builder<String, Metric> metricsBuilder = ImmutableMap.builder();
        for (final Map.Entry<String, MetricData> metricDataEntry : metricData.entrySet()) {
            final String metricName = metricDataEntry.getKey();
            final MetricData metricDatum = metricDataEntry.getValue();
            metricsBuilder.put(
                    metricName,
                    createNewMetricBuilder()
                            .setValues(metricDatum.getSamples())
                            .setStatistics(metricDatum.getStatistics())
                            .build());
        }

        // Build the list of the DefaultMetric instances
        return metricsBuilder.build();
    }

    private void parseNumericalSamples(
            final String metricName,
            final Map<String, MetricData> metricData,
            final List<Double> samples) {
        MetricData metricDatum = metricData.get(metricName);
        if (metricDatum == null) {
            metricDatum = new MetricData();
            metricData.put(metricName, metricDatum);
        }
        final ImmutableList.Builder<Quantity> quantities =
                ImmutableList.builderWithExpectedSize(samples.size());
        for (final double sample : samples) {
            quantities.add(
                    ThreadLocalBuilder.build(
                            DefaultQuantity.Builder.class,
                            b -> b.setValue(sample)));
        }
        metricDatum.addSamples(quantities.build());
    }

    private void parseNumericalStatistics(
            final String metricName,
            final Map<String, MetricData> metricData,
            final ClientV3.AugmentedHistogram augmentedHistogram) {
        MetricData metricDatum = metricData.get(metricName);
        if (metricDatum == null) {
            metricDatum = new MetricData();
            metricData.put(metricName, metricDatum);
        }

        final long populationSize = augmentedHistogram.getEntriesList().stream()
                .map(e -> (long) e.getCount())
                .reduce(Long::sum)
                .orElse(0L);
        final ImmutableList.Builder<AggregatedData> statistics = ImmutableList.builder();

        statistics.add(ThreadLocalBuilder.build(AggregatedData.Builder.class, bldr ->
                bldr.setStatistic(STATISTIC_FACTORY.getStatistic("min"))
                        .setIsSpecified(false)
                        .setValue(ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b -> b.setValue(augmentedHistogram.getMin())))
                        .setPopulationSize(populationSize)));

        statistics.add(ThreadLocalBuilder.build(AggregatedData.Builder.class, bldr ->
                bldr.setStatistic(STATISTIC_FACTORY.getStatistic("max"))
                        .setIsSpecified(false)
                        .setValue(ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b -> b.setValue(augmentedHistogram.getMax())))
                        .setPopulationSize(populationSize)));

        statistics.add(ThreadLocalBuilder.build(AggregatedData.Builder.class, bldr ->
                bldr.setStatistic(STATISTIC_FACTORY.getStatistic("count"))
                        .setIsSpecified(false)
                        .setValue(ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b -> b.setValue((double) populationSize)))
                        .setPopulationSize(populationSize)));

        statistics.add(ThreadLocalBuilder.build(AggregatedData.Builder.class, bldr ->
                bldr.setStatistic(STATISTIC_FACTORY.getStatistic("sum"))
                        .setIsSpecified(false)
                        .setValue(ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b -> b.setValue(augmentedHistogram.getSum())))
                        .setPopulationSize(populationSize)));

        if (populationSize != 0) {
            statistics.add(ThreadLocalBuilder.build(AggregatedData.Builder.class, bldr ->
                    bldr.setStatistic(STATISTIC_FACTORY.getStatistic("mean"))
                            .setIsSpecified(false)
                            .setValue(ThreadLocalBuilder.build(
                                    DefaultQuantity.Builder.class,
                                    b -> b.setValue(augmentedHistogram.getSum() / populationSize)))
                            .setPopulationSize(populationSize)));
        }

        statistics.add(ThreadLocalBuilder.build(AggregatedData.Builder.class, bldr ->
                bldr.setStatistic(STATISTIC_FACTORY.getStatistic("histogram"))
                        .setIsSpecified(false)
                        .setValue(ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b -> b.setValue(1.0)))
                        .setPopulationSize(populationSize)
                        .setSupportingData(ThreadLocalBuilder.build(
                                HistogramStatistic.HistogramSupportingData.Builder.class,
                                b -> {
                                    final HistogramStatistic.Histogram histogram = new HistogramStatistic.Histogram();
                                    augmentedHistogram.getEntriesList().forEach(
                                            e -> histogram.recordPacked(
                                                    e.getBucket(),
                                                    e.getCount()));
                                    b.setHistogramSnapshot(histogram.getSnapshot());
                                }))));

        metricDatum.addStatistics(statistics.build());
    }

    private DefaultMetric.Builder createNewMetricBuilder() {
        return new DefaultMetric.Builder()
                .setType(MetricType.GAUGE);
    }

    private ImmutableMap<String, String> buildDimensions(final ClientV3.Record record) {
        final ImmutableMap.Builder<String, String> dimensions = ImmutableMap.builder();
        for (final ClientV3.DimensionEntry dimensionEntry : record.getDimensionsList()) {
            dimensions.put(
                    decodeRequiredIdentifier(dimensionEntry.getName()),
                    decodeRequiredIdentifier(dimensionEntry.getValue()));
        }
        return dimensions.build();
    }

    private String decodeRequiredIdentifier(final ClientV3.Identifier identifier) {
        @Nullable final String identifierAsString = decodeIdentifier(identifier);
        if (identifierAsString == null) {
            throw new IllegalArgumentException("Required identifier is not set");
        }
        return identifierAsString;
    }

    @Nullable
    private String decodeIdentifier(final ClientV3.Identifier identifier) {
        switch (identifier.getValueCase()) {
            case STRINGVALUE:
                return identifier.getStringValue();
            case VALUE_NOT_SET:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported identifier type");
        }
    }

    private static final class MetricData {

        void addSamples(final Collection<Quantity> samples) {
            _metricSamples.addAll(samples);
        }

        void addStatistics(final Collection<AggregatedData> statistics) {
            _metricStatistics.addAll(statistics);
        }

        ImmutableList<Quantity> getSamples() {
            return _metricSamples.build();
        }

        ImmutableList<AggregatedData> getStatistics() {
            return _metricStatistics.build();
        }

        MetricData() {}

        private final ImmutableList.Builder<Quantity> _metricSamples = ImmutableList.builder();
        private final ImmutableList.Builder<AggregatedData> _metricStatistics = ImmutableList.builder();
    }
}
