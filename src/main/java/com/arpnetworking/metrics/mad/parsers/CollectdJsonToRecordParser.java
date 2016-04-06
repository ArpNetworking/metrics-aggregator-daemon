/**
 * Copyright 2016 Smartsheet
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
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Parses Collectd JSON data as a {@link Record}.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class CollectdJsonToRecordParser implements Parser<List<DefaultRecord.Builder>> {
    /**
     * Parses a collectd POST body.
     *
     * @param data byte array of json encoded data
     * @return A list of {@link DefaultRecord.Builder}
     * @throws ParsingException if the body is not parsable as collectd formatted json data
     */
    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    public List<DefaultRecord.Builder> parse(final byte[] data) throws ParsingException {
        try {
            final List<CollectdRecord> records = OBJECT_MAPPER.readValue(data, new TypeReference<List<CollectdRecord>>() { });
            final List<DefaultRecord.Builder> builders = Lists.newArrayList();
            for (final CollectdRecord record : records) {
                final DefaultRecord.Builder builder = new DefaultRecord.Builder();
                final Map<String, Metric> metrics = Maps.newHashMap();
                builder.setHost(record.getHost())
                        .setId(UUID.randomUUID().toString())
                        .setAnnotations(Collections.emptyMap())
                        .setTime(record.getTime());

                final String plugin = record.getPlugin();
                final String pluginInstance = record.getPluginInstance();
                final String type = record.getType();
                final String typeInstance = record.getTypeInstance();

                for (final CollectdRecord.Sample sample : record.getSamples()) {
                    final String metricName = computeMetricName(plugin, pluginInstance, type, typeInstance, sample.getDsName());
                    final MetricType metricType = mapDsType(sample.getDsType());
                    final Metric metric = new DefaultMetric.Builder()
                            .setType(metricType)
                            .setValues(Collections.singletonList(
                                    new Quantity.Builder().setValue(sample.getValue()).build()))
                            .build();
                    metrics.merge(metricName, metric, CollectdJsonToRecordParser::mergeMetrics);
                }
                builder.setMetrics(metrics);
                builders.add(builder);
            }
            return builders;
        } catch (final IOException ex) {
            // CHECKSTYLE.OFF: IllegalInstantiation - Approved for byte[] to String
            throw new ParsingException(
                    String.format("Error parsing collectd json; data=%s", new String(data, Charsets.UTF_8)),
                    ex);
            // CHECKSTYLE.ON: IllegalInstantiation
        }
    }

    private String computeMetricName(
            final String plugin,
            final String pluginInstance,
            final String type,
            final String typeInstance,
            final String dsName) {
        final StringBuilder builder = new StringBuilder();
        builder.append(plugin);
        if (pluginInstance != null && pluginInstance.length() > 0) {
            builder.append("/");
            builder.append(pluginInstance);
        }
        builder.append("/");
        builder.append(type);
        if (typeInstance != null && typeInstance.length() > 0) {
            builder.append("/");
            builder.append(typeInstance);
        }
        if (dsName != null && dsName.length() > 0) {
            builder.append("/");
            builder.append(dsName);
        }
        return builder.toString();
    }

    private MetricType mapDsType(final String type) {
        switch (type) {
            case "gauge":
                return MetricType.GAUGE;
            case "absolute":
                // This is an odd type.  It is a counter that is reset on read and divided by the last time.
                return MetricType.COUNTER;
            case "counter":
                return MetricType.COUNTER;
            case "derive":
                return MetricType.COUNTER;
            default:
                return MetricType.GAUGE;
        }
    }

    private static Metric mergeMetrics(final Metric m1, final Metric m2) {
        final ArrayList<Quantity> combinedValues = Lists.newArrayList(m1.getValues());
        combinedValues.addAll(m2.getValues());
        return new DefaultMetric.Builder()
                .setType(m1.getType())
                .setValues(combinedValues)
                .build();
    }

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();

    static {
        final SimpleModule queryLogParserModule = new SimpleModule("CollectdJsonParser");
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.registerModule(queryLogParserModule);
    }

    /**
     * Represents one record in a Collectd post body.
     */
    @Loggable
    public static final class CollectdRecord {
        public String getHost() {
            return _host;
        }

        public DateTime getTime() {
            return _time;
        }

        public String getPlugin() {
            return _plugin;
        }

        public String getPluginInstance() {
            return _pluginInstance;
        }

        public String getType() {
            return _type;
        }

        public String getTypeInstance() {
            return _typeInstance;
        }

        public List<Sample> getSamples() {
            return _samples;
        }

        private CollectdRecord(final Builder builder) {
            _host = builder._host;
            _time = new DateTime(Math.round(builder._time * 1000));
            _plugin = builder._plugin;
            _pluginInstance = builder._pluginInstance;
            _type = builder._type;
            _typeInstance = builder._typeInstance;
            _samples = Lists.newArrayListWithExpectedSize(builder._values.size());

            final Iterator<Double> valuesIterator = builder._values.iterator();
            final Iterator<String> typesIterator = builder._dsTypes.iterator();
            final Iterator<String> namesIterator = builder._dsNames.iterator();
            while (valuesIterator.hasNext() && typesIterator.hasNext() && namesIterator.hasNext()) {
                _samples.add(new Sample(valuesIterator.next(), typesIterator.next(), namesIterator.next()));
            }
        }

        private final String _host;
        private final DateTime _time;
        private final String _plugin;
        private final String _pluginInstance;
        private final String _type;
        private final String _typeInstance;
        private final List<Sample> _samples;

        /**
         * Builder for the {@link CollectdRecord} class.
         */
        public static final class Builder extends OvalBuilder<CollectdRecord> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(CollectdRecord::new);
            }

            /**
             * Sets the host.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setHost(final String value) {
                _host = value;
                return this;
            }

            /**
             * Sets the time.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setTime(final double value) {
                _time = value;
                return this;
            }

            /**
             * Sets the plugin.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setPlugin(final String value) {
                _plugin = value;
                return this;
            }

            /**
             * Sets the plugin instance.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("plugin_instance")
            public Builder setPluginInstance(final String value) {
                _pluginInstance = value;
                return this;
            }

            /**
             * Sets the type.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setType(final String value) {
                _type = value;
                return this;
            }

            /**
             * Sets the sample values.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setValues(final List<Double> value) {
                _values = value;
                return this;
            }

            /**
             * Sets the sample DS types.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("dstypes")
            public Builder setDsTypes(final List<String> value) {
                _dsTypes = value;
                return this;
            }

            /**
             * Sets the sample DS names.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("dsnames")
            public Builder setDsNames(final List<String> value) {
                _dsNames = value;
                return this;
            }

            /**
             * Sets the type instance.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("type_instance")
            public Builder setTypeInstance(final String value) {
                _typeInstance = value;
                return this;
            }

            @NotNull
            private String _host;
            private double _time;
            @NotNull
            private String _plugin;
            @NotNull
            private String _pluginInstance;
            @NotNull
            private String _type;
            @NotNull
            private String _typeInstance;
            @NotNull
            private List<Double> _values = Collections.emptyList();
            @NotNull
            private List<String> _dsTypes = Collections.emptyList();
            @NotNull
            private List<String> _dsNames = Collections.emptyList();
        }

        /**
         * Represents a single sample in a collectd metric post.
         */
        public static final class Sample {
            public double getValue() {
                return _value;
            }

            public String getDsType() {
                return _dsType;
            }

            public String getDsName() {
                return _dsName;
            }

            /**
             * Public constructor.
             *
             * @param value  The value
             * @param dsType The DS type
             * @param dsName The DS name
             */
            public Sample(final double value, final String dsType, final String dsName) {
                _value = value;
                _dsType = dsType;
                _dsName = dsName;
            }

            private final double _value;
            private final String _dsType;
            private final String _dsName;
        }
    }
}
