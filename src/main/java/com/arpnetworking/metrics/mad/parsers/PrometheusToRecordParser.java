package com.arpnetworking.metrics.mad.parsers;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.uuidfactory.UuidFactory;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.*;
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import com.arpnetworking.metrics.prometheus.Types.TimeSeries;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

public class PrometheusToRecordParser implements Parser<List<Record>, HttpRequest> {
    @Override
    public List<Record> parse(HttpRequest data) throws ParsingException {
        final List<Record> records = Lists.newArrayList();
        try {
            final byte[] uncompressed = Snappy.uncompress(data.getBody().toArray());
            final Remote.WriteRequest writeRequest = Remote.WriteRequest.parseFrom(uncompressed);
            for (final TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                String name = null;
                ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
                for (Types.Label label : timeSeries.getLabelsList()) {
                    if (label.getName().equals("__name__"))
                        name = label.getValue();
                    else
                        dimensionsBuilder.put(label.getName(), label.getValue());
                }
                final ImmutableMap<String, String> immutableDimensions = dimensionsBuilder.build();
                final String metricName = name;
                for (final Types.Sample sample : timeSeries.getSamplesList()) {
                    Record record = ThreadLocalBuilder.build(
                            DefaultRecord.Builder.class,
                            b -> b.setId(UUID.randomUUID().toString())
                                    .setTime(
                                            ZonedDateTime.ofInstant(
                                                    Instant.ofEpochMilli(sample.getTimestamp()),
                                                    ZoneOffset.UTC))
                                    .setMetrics(createMetric(metricName, sample))
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

    private ImmutableMap<String, ? extends Metric> createMetric(String name, Types.Sample sample) {

        Metric metric = ThreadLocalBuilder.build(
                DefaultMetric.Builder.class,
                p -> p
                        .setType(MetricType.GAUGE)
                        .setValues(ImmutableList.of(createQuantity(sample)))
                        .build()
        );
        return ImmutableMap.of(name, metric);
    }

    private Quantity createQuantity(Types.Sample sample) {
        return ThreadLocalBuilder.build(
                Quantity.Builder.class,
                p -> p
                        .setValue(sample.getValue())
        );
    }

}
