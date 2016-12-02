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
package com.arpnetworking.metrics.mad.parsers;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.EnumerationDeserializer;
import com.arpnetworking.commons.jackson.databind.EnumerationDeserializerStrategyUsingToUpperCase;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.json.Version2c;
import com.arpnetworking.metrics.mad.model.json.Version2d;
import com.arpnetworking.metrics.mad.model.json.Version2e;
import com.arpnetworking.metrics.mad.model.json.Version2f;
import com.arpnetworking.metrics.mad.model.json.Version2fSteno;
import com.arpnetworking.metrics.mad.model.json.Version2g;
import com.arpnetworking.metrics.mad.model.json.Version2g.CompositeUnit;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.sf.oval.exception.ConstraintsViolatedException;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Implementation of <code>Parser</code> for the JSON metrics formats. The
 * format represents each <code>Record</code> instance with one json object per
 * line. Specifications for each version of the query log can be found in the
 * <code>metrics-client-doc</code> repository.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public final class JsonToRecordParser implements Parser<Record, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Record parse(final byte[] data) throws ParsingException {
        // Attempt to parse the data as JSON to distinguish between the legacy
        // format and the current JSON format
        final JsonNode jsonNode;
        try {
            jsonNode = OBJECT_MAPPER.readTree(data);
        } catch (final IOException ex) {
            throw new ParsingException("Unsupported non-json format", data);
        }

        // If it's JSON extract the version and parse accordingly
        final Optional<JsonNode> dataNode = Optional.ofNullable(jsonNode.get(DATA_KEY));
        Optional<JsonNode> versionNode = Optional.ofNullable(jsonNode.get(VERSION_KEY));
        if (dataNode.isPresent()) {
            final Optional<JsonNode> dataVersionNode = Optional.ofNullable(dataNode.get().get(VERSION_KEY));
            if (dataVersionNode.isPresent()) {
                versionNode = dataVersionNode;
            }
        }
        if (!versionNode.isPresent()) {
            throw new ParsingException("Unable to determine version, version node not found", data);
        }
        final String version = versionNode.get().textValue().toLowerCase(Locale.getDefault());
        try {
            switch (version) {
                case "2g":
                    return parseV2gLogLine(jsonNode);
                case "2f":
                    if (dataNode.isPresent()) {
                        return parseV2fStenoLogLine(jsonNode);
                    } else {
                        return parseV2fLogLine(jsonNode);
                    }
                case "2e":
                    return parseV2eLogLine(jsonNode);
                case "2d":
                    return parseV2dLogLine(jsonNode);
                case "2c":
                    return parseV2cLogLine(jsonNode);
                default:
                    throw new ParsingException(String.format("Unsupported version; version=%s", version), data);
            }
        } catch (final JsonProcessingException | ConstraintsViolatedException e) {
            throw new ParsingException("Error parsing record", data, e);
        }
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2cLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2c model = OBJECT_MAPPER.treeToValue(jsonNode, Version2c.class);
        final Version2c.Annotations annotations = model.getAnnotations();
        final DateTime timestamp = getTimestampFor2c(annotations);

        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();
        putVariablesVersion2c(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2c(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2c(model.getGauges(), MetricType.GAUGE, variables);

        // NOTE: We are injecting context into dimensions, values set in annotations will be ignored
        final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
        dimensionsBuilder.put(Key.HOST_DIMENSION_KEY, _defaultHost);
        dimensionsBuilder.put(Key.SERVICE_DIMENSION_KEY, _defaultService);
        dimensionsBuilder.put(Key.CLUSTER_DIMENSION_KEY, _defaultCluster);

        return new DefaultRecord.Builder()
                .setMetrics(variables.build())
                .setId(UUID.randomUUID().toString())
                .setTime(timestamp)
                .setAnnotations(ImmutableMap.copyOf(annotations.getOtherAnnotations()))
                .setDimensions(dimensionsBuilder.build())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2dLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2d model = OBJECT_MAPPER.treeToValue(jsonNode, Version2d.class);
        final Version2d.Annotations annotations = model.getAnnotations();
        final DateTime timestamp = annotations.getFinalTimestamp();

        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();
        putVariablesVersion2d(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2d(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2d(model.getGauges(), MetricType.GAUGE, variables);

        // NOTE: We are injecting context into dimensions, values set in annotations will be ignored
        final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
        dimensionsBuilder.put(Key.HOST_DIMENSION_KEY, _defaultHost);
        dimensionsBuilder.put(Key.SERVICE_DIMENSION_KEY, _defaultService);
        dimensionsBuilder.put(Key.CLUSTER_DIMENSION_KEY, _defaultCluster);

        return new DefaultRecord.Builder()
                .setMetrics(variables.build())
                .setId(UUID.randomUUID().toString())
                .setTime(timestamp)
                .setAnnotations(ImmutableMap.copyOf(annotations.getOtherAnnotations()))
                .setDimensions(dimensionsBuilder.build())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2eLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2e model = OBJECT_MAPPER.treeToValue(jsonNode, Version2e.class);
        final Version2e.Data data = model.getData();
        final Version2e.Annotations annotations = data.getAnnotations();
        final DateTime timestamp = annotations.getFinalTimestamp();

        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();
        putVariablesVersion2e(data.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2e(data.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2e(data.getGauges(), MetricType.GAUGE, variables);

        // NOTE: We are injecting context into dimensions, values set in annotations will be ignored
        final ImmutableMap.Builder<String, String> dimensionsBuilder = ImmutableMap.builder();
        dimensionsBuilder.put(Key.HOST_DIMENSION_KEY, _defaultHost);
        dimensionsBuilder.put(Key.SERVICE_DIMENSION_KEY, _defaultService);
        dimensionsBuilder.put(Key.CLUSTER_DIMENSION_KEY, _defaultCluster);

        return new DefaultRecord.Builder()
                .setMetrics(variables.build())
                .setId(UUID.randomUUID().toString())
                .setTime(timestamp)
                .setAnnotations(annotations.getOtherAnnotations())
                .setDimensions(dimensionsBuilder.build())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2fLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2f model = OBJECT_MAPPER.treeToValue(jsonNode, Version2f.class);
        final Version2f.Annotations annotations = model.getAnnotations();
        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();

        putVariablesVersion2f(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2f(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2f(model.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables.build())
                .setTime(annotations.getEnd())
                .setId(annotations.getId())
                .setAnnotations(annotations.getOtherAnnotations())
                .setDimensions(extractLegacyDimensions(annotations.getOtherAnnotations()))
                .build();
    }

    // NOTE: Package private for testing
    /* package private */ com.arpnetworking.metrics.mad.model.Record parseV2fStenoLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2fSteno model = OBJECT_MAPPER.treeToValue(jsonNode, Version2fSteno.class);
        final Version2fSteno.Data data = model.getData();
        final Version2fSteno.Context context = model.getContext();
        final Version2fSteno.Annotations annotations = data.getAnnotations();

        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();
        putVariablesVersion2fSteno(data.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2fSteno(data.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2fSteno(data.getGauges(), MetricType.GAUGE, variables);

        final ImmutableMap.Builder<String, String> annotationsBuilder = ImmutableMap.builder();
        annotationsBuilder.putAll(annotations.getOtherAnnotations());
        annotationsBuilder.put(PREFIXED_HOST_KEY, context.getHost());
        final ImmutableMap<String, String> flatAnnotations = annotationsBuilder.build();

        return new DefaultRecord.Builder()
                .setMetrics(variables.build())
                .setTime(annotations.getEnd())
                .setId(model.getId())
                .setAnnotations(flatAnnotations)
                .setDimensions(extractLegacyDimensions(flatAnnotations))
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2gLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2g model = OBJECT_MAPPER.treeToValue(jsonNode, Version2g.class);
        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();

        putVariablesVersion2g(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2g(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2g(model.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables.build())
                .setTime(model.getEnd())
                .setId(model.getId())
                .setAnnotations(ImmutableMap.copyOf(model.getAnnotations()))
                .setDimensions(ImmutableMap.copyOf(model.getDimensions()))
                .build();
    }

    private static ImmutableMap<String, String> extractLegacyDimensions(final Map<String, String> annotations) {

        final ImmutableMap.Builder<String, String> defaultDimensions = ImmutableMap.builder();
        for (final Map.Entry<String, String> dimensionName : LEGACY_DIMENSION_MAP.entrySet()) {
            final String dimensionValue = annotations.get(dimensionName.getKey());
            if (null != dimensionValue) {
                defaultDimensions.put(dimensionName.getValue(), dimensionValue);
            }
        }

        return defaultDimensions.build();
    }

    /**
     * Get the existing <code>Unit</code> that corresponds to the compound unit, or null if the compound unit has no
     * <code>Unit</code> analogue.
     *
     * @param compositeUnit The <code>CompoundUnit</code> for which to find the existing analogue.
     * @return The existing <code>Unit</code> to which the <code>CompoundUnit</code> maps.
     */
    @Nullable
    private static Unit getLegacyUnit(final CompositeUnit compositeUnit) {
        return LEGACY_UNIT_MAP.getOrDefault(compositeUnit, null);
    }

    private static final String PREFIXED_HOST_KEY = "_host";
    private static final String PREFIXED_SERVICE_KEY = "_service";
    private static final String PREFIXED_CLUSTER_KEY = "_cluster";
    private static final ImmutableMap<String, String> LEGACY_DIMENSION_MAP = ImmutableMap.of(
            PREFIXED_HOST_KEY, Key.HOST_DIMENSION_KEY,
            PREFIXED_SERVICE_KEY, Key.SERVICE_DIMENSION_KEY,
            PREFIXED_CLUSTER_KEY, Key.CLUSTER_DIMENSION_KEY
    );

    private static void putVariablesVersion2c(
            final Map<String, List<String>> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, List<String>> entry : elements.entrySet()) {
            final List<String> element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(
                    Lists.transform(
                            element,
                            VERSION_2C_SAMPLE_TO_QUANTITY)
                    .stream()
                    .filter(Predicates.notNull()::apply)
                    .collect(Collectors.toList()));
            variables.put(
                    entry.getKey(),
                    new DefaultMetric.Builder()
                            .setType(metricKind)
                            .setValues(quantities)
                            .build());
        }
    }

    private static void putVariablesVersion2d(
            final Map<String, Version2d.Element> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, Version2d.Element> entry : elements.entrySet()) {
            final Version2d.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(
                    Lists.transform(
                            element.getValues(),
                            VERSION_2D_SAMPLE_TO_QUANTITY)
                    .stream()
                    .filter(Predicates.notNull()::apply)
                    .collect(Collectors.toList()));
            variables.put(
                    entry.getKey(),
                    new DefaultMetric.Builder()
                            .setType(metricKind)
                            .setValues(quantities)
                            .build());
        }
    }

    private static void putVariablesVersion2e(
            final Map<String, Version2e.Element> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, Version2e.Element> entry : elements.entrySet()) {
            final Version2e.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(
                    Lists.transform(
                            element.getValues(),
                            VERSION_2E_SAMPLE_TO_QUANTITY)
                    .stream()
                    .filter(Predicates.notNull()::apply)
                    .collect(Collectors.toList()));
            variables.put(
                    entry.getKey(),
                    new DefaultMetric.Builder()
                            .setType(metricKind)
                            .setValues(quantities)
                            .build());
        }
    }

    private static void putVariablesVersion2f(
            final Map<String, Version2f.Element> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, Version2f.Element> entry : elements.entrySet()) {
            final Version2f.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(
                    Lists.transform(
                            element.getValues(),
                            VERSION_2F_SAMPLE_TO_QUANTITY)
                    .stream()
                    .filter(Predicates.notNull()::apply)
                    .collect(Collectors.toList()));
            variables.put(
                    entry.getKey(),
                    new DefaultMetric.Builder()
                            .setType(metricKind)
                            .setValues(quantities)
                            .build());
        }
    }

    private static void putVariablesVersion2fSteno(
            final Map<String, Version2fSteno.Element> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, Version2fSteno.Element> entry : elements.entrySet()) {
            final Version2fSteno.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(
                    Lists.transform(
                            element.getValues(),
                            VERSION_2F_STENO_SAMPLE_TO_QUANTITY)
                    .stream()
                    .filter(Predicates.notNull()::apply)
                    .collect(Collectors.toList()));
            variables.put(
                    entry.getKey(),
                    new DefaultMetric.Builder()
                            .setType(metricKind)
                            .setValues(quantities)
                            .build());
        }
    }

    private static void putVariablesVersion2g(
            final Map<String, Version2g.Element> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, Version2g.Element> entry : elements.entrySet()) {
            final Version2g.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.transform(
                            element.getValues(),
                            JsonToRecordParser::version2gSampleToQuantity)
                            .stream()
                            .filter(Predicates.notNull()::apply)
                            .collect(Collectors.toList());
            variables.put(
                    entry.getKey(),
                    new DefaultMetric.Builder()
                            .setType(metricKind)
                            .setValues(quantities)
                            .build());
        }
    }

    private DateTime getTimestampFor2c(final Version2c.Annotations annotations)
            throws JsonProcessingException {
        if (annotations.getFinalTimestamp().isPresent()) {
            try {
                return timestampToDateTime(Double.parseDouble(annotations.getFinalTimestamp().get()));
                // CHECKSTYLE.OFF: EmptyBlock - Exception triggers fallback.
            } catch (final NumberFormatException nfe) {
                // CHECKSTYLE.ON: EmptyBlock
                // Ignore.
            }
        }
        if (annotations.getInitTimestamp().isPresent()) {
            try {
                return timestampToDateTime(Double.parseDouble(annotations.getInitTimestamp().get()));
                // CHECKSTYLE.OFF: EmptyBlock - Exception triggers fallback.
            } catch (final NumberFormatException nfe) {
                // CHECKSTYLE.ON: EmptyBlock
                // Ignore.
            }
        }
        throw new JsonMappingException(null, "No timestamp found in annotations");
    }

    private static DateTime timestampToDateTime(final double seconds) {
        return new DateTime(Math.round(seconds * 1000.0), ISOChronology.getInstanceUTC());
    }

    private JsonToRecordParser(final Builder builder) {
        _defaultHost = builder._defaultHost;
        _defaultService = builder._defaultService;
        _defaultCluster = builder._defaultCluster;
    }

    private static final ImmutableMap<CompositeUnit, Unit> LEGACY_UNIT_MAP = new ImmutableMap.Builder<CompositeUnit, Unit>()
            .put(new CompositeUnit(CompositeUnit.Scale.NANO, CompositeUnit.Type.SECOND), Unit.NANOSECOND)
            .put(new CompositeUnit(CompositeUnit.Scale.MICRO, CompositeUnit.Type.SECOND), Unit.MICROSECOND)
            .put(new CompositeUnit(CompositeUnit.Scale.MILLI, CompositeUnit.Type.SECOND), Unit.MILLISECOND)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.SECOND), Unit.SECOND)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.MINUTE), Unit.MINUTE)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.HOUR), Unit.HOUR)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.DAY), Unit.DAY)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.WEEK), Unit.WEEK)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.BIT), Unit.BIT)
            .put(new CompositeUnit(CompositeUnit.Scale.KILO, CompositeUnit.Type.BIT), Unit.KILOBIT)
            .put(new CompositeUnit(CompositeUnit.Scale.MEGA, CompositeUnit.Type.BIT), Unit.MEGABIT)
            .put(new CompositeUnit(CompositeUnit.Scale.GIGA, CompositeUnit.Type.BIT), Unit.GIGABIT)
            .put(new CompositeUnit(CompositeUnit.Scale.TERA, CompositeUnit.Type.BIT), Unit.TERABIT)
            .put(new CompositeUnit(CompositeUnit.Scale.PETA, CompositeUnit.Type.BIT), Unit.PETABIT)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.BYTE), Unit.BYTE)
            .put(new CompositeUnit(CompositeUnit.Scale.KILO, CompositeUnit.Type.BYTE), Unit.KILOBYTE)
            .put(new CompositeUnit(CompositeUnit.Scale.MEGA, CompositeUnit.Type.BYTE), Unit.MEGABYTE)
            .put(new CompositeUnit(CompositeUnit.Scale.GIGA, CompositeUnit.Type.BYTE), Unit.GIGABYTE)
            .put(new CompositeUnit(CompositeUnit.Scale.TERA, CompositeUnit.Type.BYTE), Unit.TERABYTE)
            .put(new CompositeUnit(CompositeUnit.Scale.PETA, CompositeUnit.Type.BYTE), Unit.PETABYTE)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.KELVIN), Unit.KELVIN)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.CELSIUS), Unit.CELCIUS)
            .put(new CompositeUnit(CompositeUnit.Scale.ONE, CompositeUnit.Type.FAHRENHEIT), Unit.FAHRENHEIT)

            .put(new CompositeUnit(null, CompositeUnit.Type.SECOND), Unit.SECOND)
            .put(new CompositeUnit(null, CompositeUnit.Type.MINUTE), Unit.MINUTE)
            .put(new CompositeUnit(null, CompositeUnit.Type.HOUR), Unit.HOUR)
            .put(new CompositeUnit(null, CompositeUnit.Type.DAY), Unit.DAY)
            .put(new CompositeUnit(null, CompositeUnit.Type.WEEK), Unit.WEEK)
            .put(new CompositeUnit(null, CompositeUnit.Type.BIT), Unit.BIT)
            .put(new CompositeUnit(null, CompositeUnit.Type.BYTE), Unit.BYTE)
            .put(new CompositeUnit(null, CompositeUnit.Type.KELVIN), Unit.KELVIN)
            .put(new CompositeUnit(null, CompositeUnit.Type.CELSIUS), Unit.CELCIUS)
            .put(new CompositeUnit(null, CompositeUnit.Type.FAHRENHEIT), Unit.FAHRENHEIT)
            .build();

    private final String _defaultHost;
    private final String _defaultService;
    private final String _defaultCluster;

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();
    private static final String DATA_KEY = "data";
    private static final String VERSION_KEY = "version";
    private static final String LOCAL_HOST_NAME;
    private static final Logger INVALID_SAMPLE_LOGGER = LoggerFactory.getRateLimitLogger(JsonToRecordParser.class, Duration.ofSeconds(30));

    private static final Function<String, Quantity> VERSION_2C_SAMPLE_TO_QUANTITY = sample -> {
        if (sample != null) {
            try {
                final double value = Double.parseDouble(sample);
                if (Double.isFinite(value)) {
                    return new Quantity.Builder().setValue(value).build();
                } else {
                    // TODO(barp): Create a counter for invalid metrics [AINT-680]
                    INVALID_SAMPLE_LOGGER
                            .warn()
                            .setMessage("Invalid sample for metric")
                            .addData("value", sample)
                            .log();
                    return null;
                }
            } catch (final NumberFormatException nfe) {
                return null;
            }
        } else {
            return null;
        }
    };

    private static final Function<Version2d.Sample, Quantity> VERSION_2D_SAMPLE_TO_QUANTITY = sample -> {
        if (sample != null) {
            if (Double.isFinite(sample.getValue())) {
                return new Quantity.Builder()
                        .setValue(sample.getValue())
                        .setUnit(sample.getUnit().orElse(null))
                        .build();
            } else {
                // TODO(barp): Create a counter for invalid metrics [AINT-680]
                INVALID_SAMPLE_LOGGER
                        .warn()
                        .setMessage("Invalid sample for metric")
                        .addData("value", sample.getValue())
                        .log();
                return null;
            }
        } else {
            return null;
        }
    };

    private static final Function<Version2e.Sample, Quantity> VERSION_2E_SAMPLE_TO_QUANTITY = sample -> {
        if (sample != null) {
            if (Double.isFinite(sample.getValue())) {
                return new Quantity.Builder()
                        .setValue(sample.getValue())
                        .setUnit(sample.getUnit().orElse(null))
                        .build();
            } else {
                // TODO(barp): Create a counter for invalid metrics [AINT-680]
                INVALID_SAMPLE_LOGGER
                        .warn()
                        .setMessage("Invalid sample for metric")
                        .addData("value", sample.getValue())
                        .log();
                return null;
            }
        } else {
            return null;
        }
    };

    private static final Function<Version2f.Sample, Quantity> VERSION_2F_SAMPLE_TO_QUANTITY = sample -> {
        if (sample != null) {
            if (Double.isFinite(sample.getValue())) {
                return new Quantity.Builder()
                        .setValue(sample.getValue())
                        .setUnit(Iterables.getFirst(sample.getUnitNumerators(), null))
                        // TODO(vkoskela): Support compound units in Tsd Aggregator [AINT-679]
                        //.setNumeratorUnits(sample.getUnitNumerators())
                        //.setDenominatorUnits(sample.getUnitDenominators())
                        .build();
            } else {
                // TODO(barp): Create a counter for invalid metrics [AINT-680]
                INVALID_SAMPLE_LOGGER
                        .warn()
                        .setMessage("Invalid sample for metric")
                        .addData("value", sample.getValue())
                        .log();
                return null;
            }
        } else {
            return null;
        }
    };

    private static final Function<Version2fSteno.Sample, Quantity> VERSION_2F_STENO_SAMPLE_TO_QUANTITY = sample -> {
        if (sample != null) {
            if (Double.isFinite(sample.getValue())) {
                return new Quantity.Builder()
                        .setValue(sample.getValue())
                        .setUnit(Iterables.getFirst(sample.getUnitNumerators(), null))
                                // TODO(vkoskela): Support compound units in Tsd Aggregator [AINT-679]
                        //.setNumeratorUnits(sample.getUnitNumerators())
                        //.setDenominatorUnits(sample.getUnitDenominators())
                        .build();
            } else {
                // TODO(barp): Create a counter for invalid metrics [AINT-680]
                INVALID_SAMPLE_LOGGER
                        .warn()
                        .setMessage("Invalid sample for metric")
                        .addData("value", sample.getValue())
                        .log();
                return null;
            }
        } else {
            return null;
        }
    };

    private static Quantity version2gSampleToQuantity(final Version2g.Sample sample) {
        if (sample != null) {
            if (Double.isFinite(sample.getValue())) {
                final CompositeUnit sampleUnit = sample.getUnit2g() != null
                        ? Iterables.getFirst(sample.getUnit2g().getNumerators(), null)
                        : null;

                return new Quantity.Builder()
                        .setValue(sample.getValue())
                        .setUnit(getLegacyUnit(sampleUnit))
                        // TODO(vkoskela): Support compound units in Tsd Aggregator
                        //.setNumerator(sampleNumerator)  // same as sampleUnit above
                        //.setDenominator(sampleDenominator)
                        .build();
            } else {
                // TODO(barp): Create a counter for invalid metrics
                INVALID_SAMPLE_LOGGER
                        .warn()
                        .setMessage("Invalid sample for metric")
                        .addData("value", sample.getValue())
                        .log();
                return null;
            }
        } else {
            return null;
        }
    };

    static {
        final SimpleModule queryLogParserModule = new SimpleModule("QueryLogParser");
        queryLogParserModule.addDeserializer(
                Unit.class,
                EnumerationDeserializer.newInstance(
                        Unit.class,
                        EnumerationDeserializerStrategyUsingToUpperCase.newInstance()));
        queryLogParserModule.addDeserializer(
                CompositeUnit.Type.class,
                EnumerationDeserializer.newInstance(
                        CompositeUnit.Type.class,
                        EnumerationDeserializerStrategyUsingToUpperCase.newInstance()));
        queryLogParserModule.addDeserializer(
                CompositeUnit.Scale.class,
                EnumerationDeserializer.newInstance(
                        CompositeUnit.Scale.class,
                        EnumerationDeserializerStrategyUsingToUpperCase.newInstance()));
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.registerModule(queryLogParserModule);
        OBJECT_MAPPER.registerModule(new AfterburnerModule());

        final String localHostName;
        try {
            // Application will fail to start if the host name can not be determined.
            localHostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (final UnknownHostException e) {
            throw Throwables.propagate(e);
        }
        LOCAL_HOST_NAME = localHostName;
    }

    /**
     * Implementation of <code>Builder</code> for {@link JsonToRecordParser}.
     */
    public static final class Builder extends OvalBuilder<JsonToRecordParser> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonToRecordParser::new);
        }

        /**
         * The default host. Set to the name of the local host if left unset here.
         *
         * @param value The default host.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setDefaultHost(final String value) {
            _defaultHost = value;
            return this;
        }

        /**
         * The default service.
         *
         * @param value The default service.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setDefaultService(final String value) {
            _defaultService = value;
            return this;
        }

        /**
         * The default cluster.
         *
         * @param value The default cluster.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setDefaultCluster(final String value) {
            _defaultCluster = value;
            return this;
        }

        // NOTE: There are no constraints on these values as they are only
        // required for parsing pre-2F formats and the lack of constraints
        // allows 2F only consumer to safely omit them.
        private String _defaultHost = LOCAL_HOST_NAME;
        private String _defaultService;
        private String _defaultCluster;
    }
}
