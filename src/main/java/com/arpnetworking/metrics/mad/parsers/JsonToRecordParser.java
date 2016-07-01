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
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
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
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Implementation of <code>Parser</code> for the JSON metrics formats. The
 * format represents each <code>Record</code> instance with one json object per
 * line. Specifications for each version of the query log can be found in the
 * <code>metrics-client-doc</code> repository.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
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
        final Optional<JsonNode> dataNode = Optional.fromNullable(jsonNode.get(DATA_KEY));
        Optional<JsonNode> versionNode = Optional.fromNullable(jsonNode.get(VERSION_KEY));
        if (dataNode.isPresent()) {
            final Optional<JsonNode> dataVersionNode = Optional.fromNullable(dataNode.get().get(VERSION_KEY));
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
        } catch (final JsonProcessingException e) {
            throw new ParsingException("Error parsing record", data, e);
        }
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2cLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2c model = OBJECT_MAPPER.treeToValue(jsonNode, Version2c.class);
        final Version2c.Annotations annotations = model.getAnnotations();
        final DateTime timestamp = getTimestampFor2c(annotations);

        final Map<String, Metric> variables = Maps.newHashMap();
        putVariablesVersion2c(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2c(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2c(model.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables)
                .setId(UUID.randomUUID().toString())
                .setTime(timestamp)
                .setHost(_defaultHost)
                .setService(_defaultService)
                .setCluster(_defaultCluster)
                .setAnnotations(annotations.getOtherAnnotations())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2dLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2d model = OBJECT_MAPPER.treeToValue(jsonNode, Version2d.class);
        final Version2d.Annotations annotations = model.getAnnotations();
        final DateTime timestamp = annotations.getFinalTimestamp();

        final Map<String, Metric> variables = Maps.newHashMap();
        putVariablesVersion2d(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2d(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2d(model.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables)
                .setId(UUID.randomUUID().toString())
                .setTime(timestamp)
                .setHost(_defaultHost)
                .setService(_defaultService)
                .setCluster(_defaultCluster)
                .setAnnotations(annotations.getOtherAnnotations())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2eLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2e model = OBJECT_MAPPER.treeToValue(jsonNode, Version2e.class);
        final Version2e.Data data = model.getData();
        final Version2e.Annotations annotations = data.getAnnotations();
        final DateTime timestamp = annotations.getFinalTimestamp();

        final Map<String, Metric> variables = Maps.newHashMap();
        putVariablesVersion2e(data.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2e(data.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2e(data.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables)
                .setId(UUID.randomUUID().toString())
                .setTime(timestamp)
                .setHost(_defaultHost)
                .setService(_defaultService)
                .setCluster(_defaultCluster)
                .setAnnotations(annotations.getOtherAnnotations())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */com.arpnetworking.metrics.mad.model.Record parseV2fLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2f model = OBJECT_MAPPER.treeToValue(jsonNode, Version2f.class);
        final Version2f.Annotations annotations = model.getAnnotations();
        final Map<String, Metric> variables = Maps.newHashMap();

        putVariablesVersion2f(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2f(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2f(model.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables)
                .setTime(annotations.getEnd())
                .setId(annotations.getId())
                .setHost(annotations.getHost())
                .setService(annotations.getService())
                .setCluster(annotations.getCluster())
                .setAnnotations(annotations.getOtherAnnotations())
                .build();
    }

    // NOTE: Package private for testing
    /* package private */ com.arpnetworking.metrics.mad.model.Record parseV2fStenoLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2fSteno model = OBJECT_MAPPER.treeToValue(jsonNode, Version2fSteno.class);
        final Version2fSteno.Data data = model.getData();
        final Version2fSteno.Context context = model.getContext();
        final Version2fSteno.Annotations annotations = data.getAnnotations();

        final Map<String, Metric> variables = Maps.newHashMap();
        putVariablesVersion2fSteno(data.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2fSteno(data.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2fSteno(data.getGauges(), MetricType.GAUGE, variables);

        return new DefaultRecord.Builder()
                .setMetrics(variables)
                .setTime(annotations.getEnd())
                .setId(model.getId())
                .setHost(context.getHost())
                .setService(annotations.getService())
                .setCluster(annotations.getCluster())
                .setAnnotations(annotations.getOtherAnnotations())
                .build();
    }

    private static void putVariablesVersion2c(
            final Map<String, List<String>> elements,
            final MetricType metricKind,
            final Map<String, Metric> variables) {

        for (final Map.Entry<String, List<String>> entry : elements.entrySet()) {
            final List<String> element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(Iterables.filter(
                    Lists.transform(element, VERSION_2C_SAMPLE_TO_QUANTITY),
                    Predicates.notNull()));
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
            final Map<String, Metric> variables) {

        for (final Map.Entry<String, Version2d.Element> entry : elements.entrySet()) {
            final Version2d.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(Iterables.filter(
                    Lists.transform(element.getValues(), VERSION_2D_SAMPLE_TO_QUANTITY),
                    Predicates.notNull()));
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
            final Map<String, Metric> variables) {

        for (final Map.Entry<String, Version2e.Element> entry : elements.entrySet()) {
            final Version2e.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(Iterables.filter(
                    Lists.transform(element.getValues(), VERSION_2E_SAMPLE_TO_QUANTITY),
                    Predicates.notNull()));
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
            final Map<String, Metric> variables) {

        for (final Map.Entry<String, Version2f.Element> entry : elements.entrySet()) {
            final Version2f.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(Iterables.filter(
                    Lists.transform(element.getValues(), VERSION_2F_SAMPLE_TO_QUANTITY),
                    Predicates.notNull()));
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
            final Map<String, Metric> variables) {

        for (final Map.Entry<String, Version2fSteno.Element> entry : elements.entrySet()) {
            final Version2fSteno.Element element = entry.getValue();
            final List<Quantity> quantities = Lists.newArrayList(Iterables.filter(
                    Lists.transform(element.getValues(), VERSION_2F_STENO_SAMPLE_TO_QUANTITY),
                    Predicates.notNull()));
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
        throw new JsonMappingException((Closeable) null, "No timestamp found in annotations");
    }

    private static DateTime timestampToDateTime(final double seconds) {
        return new DateTime(Math.round(seconds * 1000.0), ISOChronology.getInstanceUTC());
    }

    private JsonToRecordParser(final Builder builder) {
        _defaultHost = builder._defaultHost;
        _defaultService = builder._defaultService;
        _defaultCluster = builder._defaultCluster;
    }

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
                        .setUnit(sample.getUnit().orNull())
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
                        .setUnit(sample.getUnit().orNull())
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

    static {
        final SimpleModule queryLogParserModule = new SimpleModule("QueryLogParser");
        queryLogParserModule.addDeserializer(
                Unit.class,
                EnumerationDeserializer.newInstance(
                        Unit.class,
                        EnumerationDeserializerStrategyUsingToUpperCase.<Unit>newInstance()));
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
