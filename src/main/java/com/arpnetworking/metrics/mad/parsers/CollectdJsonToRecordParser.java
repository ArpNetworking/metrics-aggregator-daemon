/*
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

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.HttpRequest;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.Key;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import net.sf.oval.Validator;
import net.sf.oval.constraint.CheckWith;
import net.sf.oval.constraint.CheckWithCheck;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.context.OValContext;
import net.sf.oval.exception.ConstraintsViolatedException;

import java.io.IOException;
import java.io.Serial;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Parses Collectd JSON data as a {@link Record}.
 *
 * TODO(ville): Convert CollectdRecord fields to Optional.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class CollectdJsonToRecordParser implements Parser<List<Record>, HttpRequest> {

    /**
     * Parses a collectd POST body.
     *
     * @param request an HTTP request
     * @return A list of {@link DefaultRecord.Builder}
     * @throws ParsingException if the body is not parsable as collectd formatted json data
     */
    public List<Record> parse(final HttpRequest request) throws ParsingException {
        final Map<String, String> metricTags = Maps.newHashMap();
        for (final Map.Entry<String, String> header : request.getHeaders().entries()) {
            if (header.getKey().toLowerCase(Locale.ENGLISH).startsWith(TAG_PREFIX)) {
                metricTags.put(header.getKey().toLowerCase(Locale.ENGLISH).substring(TAG_PREFIX.length()), header.getValue());
            }
        }
        try {
            final List<CollectdRecord> records = OBJECT_MAPPER.readValue(request.getBody().toArray(), COLLECTD_RECORD_LIST);
            final List<Record> parsedRecords = Lists.newArrayList();
            for (final CollectdRecord record : records) {
                final Multimap<String, Metric> metrics = HashMultimap.create();

                metricTags.put(Key.HOST_DIMENSION_KEY, record.getHost());

                final String plugin = record.getPlugin();
                final String pluginInstance = record.getPluginInstance();
                final String type = record.getType();
                final String typeInstance = record.getTypeInstance();

                final List<String> dsTypes = record.getDsTypes();
                final List<String> dsNames = record.getDsNames();
                final List<Double> values = record.getValues();

                if (values != null && dsTypes != null && dsNames != null) {
                    if (values.size() == 1 && values.get(0) == null) {
                        // Ignore null values
                        continue;
                    }
                    final Iterator<Double> valuesIterator = values.iterator();
                    final Iterator<String> typesIterator = dsTypes.iterator();
                    final Iterator<String> namesIterator = dsNames.iterator();

                    while (valuesIterator.hasNext() && typesIterator.hasNext() && namesIterator.hasNext()) {
                        final String metricName = computeMetricName(plugin, pluginInstance, type, typeInstance, namesIterator.next());
                        final MetricType metricType = mapDsType(typesIterator.next());
                        // TODO(ville): Support units and normalize
                        final Metric metric = ThreadLocalBuilder.build(
                                DefaultMetric.Builder.class,
                                b1 -> b1.setType(metricType)
                                        .setValues(ImmutableList.of(
                                                ThreadLocalBuilder.build(
                                                        DefaultQuantity.Builder.class,
                                                        b2 -> b2.setValue(valuesIterator.next())))));
                        metrics.put(metricName, metric);
                    }
                }
                final Map<String, Metric> collectedMetrics = metrics.asMap()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, CollectdJsonToRecordParser::mergeMetrics));

                final Record defaultRecord = ThreadLocalBuilder.build(
                        DefaultRecord.Builder.class,
                        b -> b.setId(UUID.randomUUID().toString())
                                .setTime(ZonedDateTime.ofInstant(
                                        Instant.ofEpochMilli(Math.round(record.getTime() * 1000)),
                                        ZoneOffset.UTC))
                                .setAnnotations(ImmutableMap.copyOf(metricTags))
                                .setDimensions(ImmutableMap.copyOf(metricTags))
                                .setMetrics(ImmutableMap.copyOf(collectedMetrics)));
                parsedRecords.add(defaultRecord);
            }
            return parsedRecords;
        } catch (final IOException | ConstraintsViolatedException ex) {
            throw new ParsingException("Error parsing collectd json", request.getBody().toArray(), ex);
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
        if (!Strings.isNullOrEmpty(pluginInstance)) {
            builder.append("/");
            builder.append(pluginInstance);
        }
        builder.append("/");
        builder.append(type);
        if (!Strings.isNullOrEmpty(typeInstance)) {
            builder.append("/");
            builder.append(typeInstance);
        }
        if (!Strings.isNullOrEmpty(dsName) && !dsName.equals("value")) {
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

    private static Metric mergeMetrics(final Map.Entry<String, Collection<Metric>> entries) {
        final Collection<Metric> metrics = entries.getValue();
        if (metrics.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }
        final Metric firstMetric = metrics.iterator().next();
        if (metrics.size() == 1) {
            return firstMetric;
        } else {
            final ImmutableList.Builder<Quantity> quantities = ImmutableList.builder();
            for (final Metric metric : metrics) {
                quantities.addAll(metric.getValues());
            }
            return ThreadLocalBuilder.build(
                    DefaultMetric.Builder.class,
                    b -> b.setType(firstMetric.getType())
                            .setValues(quantities.build()));
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();
    private static final TypeReference<List<CollectdRecord>> COLLECTD_RECORD_LIST = new TypeReference<List<CollectdRecord>>() {};
    private static final String TAG_PREFIX = "x-tag-";

    static {
        OBJECT_MAPPER.configure(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature(), true);
        OBJECT_MAPPER.registerModule(new AfterburnerModule());
    }

    /**
     * Represents one record in a Collectd post body.
     */
    @Loggable
    public static final class CollectdRecord {
        public String getHost() {
            return _host;
        }

        public Double getTime() {
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

        @Nullable
        public List<Double> getValues() {
            return _values;
        }

        @Nullable
        public List<String> getDsTypes() {
            return _dsTypes;
        }

        @Nullable
        public List<String> getDsNames() {
            return _dsNames;
        }

        private CollectdRecord(final Builder builder) {
            _host = builder._host;
            _time = builder._time;
            _plugin = builder._plugin;
            _pluginInstance = builder._pluginInstance;
            _type = builder._type;
            _typeInstance = builder._typeInstance;
            _values = builder._values;
            _dsTypes = builder._dsTypes;
            _dsNames = builder._dsNames;
        }

        private final String _host;
        private final Double _time;
        private final String _plugin;
        private final String _pluginInstance;
        private final String _type;
        private final String _typeInstance;
        private final List<Double> _values;
        private final List<String> _dsTypes;
        private final List<String> _dsNames;

        /**
         * {@link com.arpnetworking.commons.builder.Builder} implementation for
         * {@link CollectdRecord}.
         */
        public static final class Builder extends ThreadLocalBuilder<CollectdRecord> {
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
             * Sets the time.  Time value is floating point epoch seconds. Required.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setTime(final Double value) {
                _time = value;
                return this;
            }

            /**
             * Sets the plugin. Required.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setPlugin(final String value) {
                _plugin = value;
                return this;
            }

            /**
             * Sets the plugin instance. Required.
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
             * Sets the type. Required.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setType(final String value) {
                _type = value;
                return this;
            }

            /**
             * Sets the sample values. Required.
             *
             * @param value Value
             * @return This builder
             */
            public Builder setValues(@Nullable final List<Double> value) {
                _values = value;
                return this;
            }

            /**
             * Sets the sample DS types. Required.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("dstypes")
            public Builder setDsTypes(@Nullable final ImmutableList<String> value) {
                _dsTypes = value;
                return this;
            }

            /**
             * Sets the sample DS names. Required.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("dsnames")
            public Builder setDsNames(@Nullable final ImmutableList<String> value) {
                _dsNames = value;
                return this;
            }

            /**
             * Sets the type instance. Required.
             *
             * @param value Value
             * @return This builder
             */
            @JsonProperty("type_instance")
            public Builder setTypeInstance(final String value) {
                _typeInstance = value;
                return this;
            }

            @Override
            protected void reset() {
                _host = null;
                _time = null;
                _plugin = null;
                _pluginInstance = null;
                _type = null;
                _typeInstance = null;
                _values = ImmutableList.of();
                _dsTypes = ImmutableList.of();
                _dsNames = ImmutableList.of();
            }

            @NotNull
            private String _host;
            // TODO(ville): Verify required as not null; see Instant.ofEpochMilli
            @NotNull
            private Double _time;
            // TODO(ville): Verify required as not null; see computeMetricName
            @NotNull
            private String _plugin;
            @NotNull
            private String _pluginInstance;
            // TODO(ville): Verify required as not null; see computeMetricName
            @NotNull
            private String _type;
            @NotNull
            private String _typeInstance;
            @Nullable
            @CheckWith(value = ValueArraysValid.class, message = "values, dstypes, and dsnames must have the same number of entries")
            private List<Double> _values = ImmutableList.of();
            @Nullable
            private ImmutableList<String> _dsTypes = ImmutableList.of();
            @Nullable
            private ImmutableList<String> _dsNames = ImmutableList.of();

            private static class ValueArraysValid implements CheckWithCheck.SimpleCheck {
                @Override
                public boolean isSatisfied(
                        final Object validatedObject,
                        final Object value,
                        final OValContext context,
                        final Validator validator) {
                    if (validatedObject instanceof Builder) {
                        final Builder builder = (Builder) validatedObject;
                        if (builder._values == null && builder._dsNames == null && builder._dsTypes == null) {
                            return true;
                        }
                        return builder._values != null && builder._dsTypes != null && builder._dsNames != null
                                && builder._values.size() == builder._dsTypes.size()
                                && builder._values.size() == builder._dsNames.size();
                    }
                    return false;
                }

                @Serial
                private static final long serialVersionUID = 1L;
            }
        }
    }
}
