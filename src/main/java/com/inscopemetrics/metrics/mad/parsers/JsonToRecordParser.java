/*
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
package com.inscopemetrics.metrics.mad.parsers;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.jackson.databind.EnumerationDeserializer;
import com.arpnetworking.commons.jackson.databind.EnumerationDeserializerStrategyUsingToUpperCase;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.inscopemetrics.metrics.common.parsers.Parser;
import com.inscopemetrics.metrics.common.parsers.exceptions.ParsingException;
import com.inscopemetrics.metrics.mad.model.DefaultMetric;
import com.inscopemetrics.metrics.mad.model.DefaultRecord;
import com.inscopemetrics.metrics.mad.model.Metric;
import com.inscopemetrics.metrics.mad.model.Record;
import com.inscopemetrics.metrics.mad.model.json.Version2g;
import com.inscopemetrics.metrics.mad.model.json.Version2g.CompositeUnit;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.inscopemetrics.tsdcore.model.MetricType;
import com.inscopemetrics.tsdcore.model.Quantity;
import com.inscopemetrics.tsdcore.model.Unit;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.exception.ConstraintsViolatedException;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Implementation of <code>Parser</code> for the JSON metrics formats. The
 * format represents each <code>Record</code> instance with one json object per
 * line. Specifications for each version of the query log can be found in the
 * <code>metrics-client-doc</code> repository.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public final class JsonToRecordParser implements Parser<Record, byte[]> {

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
        final Optional<JsonNode> versionNode = Optional.ofNullable(jsonNode.get(VERSION_KEY));
        if (!versionNode.isPresent()) {
            throw new ParsingException("Unable to determine version, version node not found", data);
        }
        final String version = versionNode.get().textValue().toLowerCase(Locale.getDefault());
        try {
            switch (version) {
                case "2g":
                    return parseV2gLogLine(jsonNode);
                default:
                    throw new ParsingException(String.format("Unsupported version; version=%s", version), data);
            }
        } catch (final JsonProcessingException | ConstraintsViolatedException e) {
            throw new ParsingException("Error parsing record", data, e);
        }
    }

    private Record parseV2gLogLine(final JsonNode jsonNode)
            throws JsonProcessingException {

        final Version2g model = OBJECT_MAPPER.treeToValue(jsonNode, Version2g.class);
        final ImmutableMap.Builder<String, Metric> variables = ImmutableMap.builder();

        putVariablesVersion2g(model.getTimers(), MetricType.TIMER, variables);
        putVariablesVersion2g(model.getCounters(), MetricType.COUNTER, variables);
        putVariablesVersion2g(model.getGauges(), MetricType.GAUGE, variables);

        return ThreadLocalBuilder.build(
                DefaultRecord.Builder.class,
                b -> b.setMetrics(variables.build())
                        .setTime(model.getEnd())
                        .setId(model.getId())
                        .setAnnotations(ImmutableMap.copyOf(model.getAnnotations()))
                        .setDimensions(ImmutableMap.copyOf(model.getDimensions())));
    }

    /**
     * Get the existing <code>Unit</code> that corresponds to the compound unit, or null if the compound unit has no
     * <code>Unit</code> analogue.
     *
     * @param compositeUnit The <code>CompoundUnit</code> for which to find the existing analogue.
     * @return The existing <code>Unit</code> to which the <code>CompoundUnit</code> maps.
     */
    @Nullable
    private static Unit getLegacyUnit(@Nullable final CompositeUnit compositeUnit) {
        return LEGACY_UNIT_MAP.getOrDefault(compositeUnit, null);
    }

    private static void putVariablesVersion2g(
            final Map<String, Version2g.Element> elements,
            final MetricType metricKind,
            final ImmutableMap.Builder<String, Metric> variables) {

        for (final Map.Entry<String, Version2g.Element> entry : elements.entrySet()) {
            final Version2g.Element element = entry.getValue();
            final ImmutableList<Quantity> quantities = element.getValues().stream()
                    .map(JsonToRecordParser::version2gSampleToQuantity)
                    .collect(Collectors.toList())
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(ImmutableList.toImmutableList());
            variables.put(
                    entry.getKey(),
                    ThreadLocalBuilder.build(
                            DefaultMetric.Builder.class,
                            b -> b.setType(metricKind)
                                    .setValues(quantities)));
        }
    }

    private JsonToRecordParser(final Builder builder) { }

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

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();
    private static final String VERSION_KEY = "version";
    private static final Logger INVALID_SAMPLE_LOGGER = LoggerFactory.getRateLimitLogger(
            JsonToRecordParser.class,
            Duration.ofSeconds(30));

    private static Quantity version2gSampleToQuantity(final Version2g.Sample sample) {
        if (sample != null) {
            if (Double.isFinite(sample.getValue())) {
                @Nullable final CompositeUnit sampleUnit = sample.getUnit2g() != null
                        ? sample.getUnit2g().getNumerators().stream().findFirst().orElse(null)
                        : null;

                return ThreadLocalBuilder.build(
                        Quantity.Builder.class,
                        b -> b.setValue(sample.getValue())
                                .setUnit(getLegacyUnit(sampleUnit)));
                // TODO(vkoskela): Support compound units in Tsd Aggregator
                //.setNumerator(sampleNumerator)  // same as sampleUnit above
                //.setDenominator(sampleDenominator)
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
    }

    static {
        final SimpleModule queryLogParserModule = new SimpleModule("MadJsonParserModule");
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
    }

    /**
     * Implementation of <code>Builder</code> for {@link JsonToRecordParser}.
     */
    public static final class Builder extends ThreadLocalBuilder<JsonToRecordParser> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonToRecordParser::new);
        }

        @Override
        protected void reset() { }
    }
}
