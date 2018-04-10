/**
 * Copyright 2018 InscopeMetrics.com
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
package com.arpnetworking.metrics.mad.sources;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.common.sources.BaseSource;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.utility.RegexAndMapReplacer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Source} which wraps another {@link Source}
 * and merges {@link Metric} instances within each {@link Record}
 * together while, optionally, removing, injecting, and modifying dimensions
 * and metrics if the name matches a regular expression with a new name generated
 * through replacement of all matches in the original name.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TransformingSource extends BaseSource {

    @Override
    public void start() {
        _source.start();
    }

    @Override
    public void stop() {
        _source.stop();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("source", _source)
                .put("tranformations", _transformations)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private TransformingSource(final Builder builder) {
        super(builder);
        _source = builder._source;
        _transformations = builder._transformations;

        _source.attach(new TransformingObserver(this, _transformations));
    }

    private final Source _source;
    private final ImmutableList<TransformationSet> _transformations;

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformingSource.class);
    private static final Splitter.MapSplitter TAG_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=');

    // NOTE: Package private for testing
    /* package private */ static final class TransformingObserver implements Observer {

        /* package private */ TransformingObserver(
                final TransformingSource source,
                final ImmutableList<TransformationSet> transformations) {
            _source = source;
            _transformations = transformations;
        }

        @Override
        public void notify(final Observable observable, final Object event) {
            if (!(event instanceof Record)) {
                LOGGER.error()
                        .setMessage("Observed unsupported event")
                        .addData("event", event)
                        .log();
                return;
            }

            // Merge the metrics in the record together
            final Record record = (Record) event;
            final Map<Key, Map<String, MergingMetric>> mergedMetrics = Maps.newHashMap();
            final LinkedHashMap<String, Map<String, String>> variablesMap = new LinkedHashMap<>();
            variablesMap.put("dimension", record.getDimensions());
            variablesMap.put("env", System.getenv());

            for (TransformationSet transformation : _transformations) {
                for (final Map.Entry<String, ? extends Metric> metric : record.getMetrics().entrySet()) {
                    boolean found = false;
                    final String metricName = metric.getKey();
                    for (final Map.Entry<Pattern, ImmutableList<String>> findAndReplace : transformation.getFindAndReplace().entrySet()) {
                        final Pattern metricPattern = findAndReplace.getKey();
                        final Matcher matcher = metricPattern.matcher(metricName);
                        if (matcher.find()) {
                            for (final String replacement : findAndReplace.getValue()) {
                                final RegexAndMapReplacer.Replacement rep =
                                        RegexAndMapReplacer.replaceAll(metricPattern, metricName, replacement, variablesMap);
                                final String replacedString = rep.getReplacement();
                                final List<String> consumedDimensions = rep.getVariablesMatched().stream()
                                        .filter(var -> var.startsWith("dimension:") || var.indexOf(':') == -1) // Only dimension vars
                                        .map(var -> var.substring(var.indexOf(":") + 1)) // Strip the prefix
                                        .collect(Collectors.toList());

                                final int tagsStart = replacedString.indexOf(';');
                                if (tagsStart == -1) {
                                    // We just have a metric name.  Optimize for this common case
                                    merge(
                                            metric.getValue(),
                                            replacedString,
                                            mergedMetrics,
                                            getModifiedDimensions(
                                                    record.getDimensions(),
                                                    Collections.emptyMap(),
                                                    consumedDimensions,
                                                    transformation));
                                } else {
                                    final String newMetricName = replacedString.substring(0, tagsStart);
                                    final Map<String, String> parsedTags = TAG_SPLITTER.split(replacedString.substring(tagsStart + 1));
                                    merge(
                                            metric.getValue(),
                                            newMetricName,
                                            mergedMetrics,
                                            getModifiedDimensions(record.getDimensions(), parsedTags, consumedDimensions, transformation));
                                }
                            }
                            //Having "found" set here means that mapping a metric to an empty list suppresses that metric
                            found = true;
                        }
                    }
                    if (!found) {
                        merge(
                                metric.getValue(),
                                metricName,
                                mergedMetrics,
                                getModifiedDimensions(record.getDimensions(), Collections.emptyMap(), ImmutableList.of(), transformation));
                    }
                }

                // Raise the merged record event with this source's observers
                // NOTE: Do not leak instances of MergingMetric since it is mutable
                for (final Map.Entry<Key, Map<String, MergingMetric>> entry : mergedMetrics.entrySet()) {
                    _source.notify(
                            ThreadLocalBuilder.build(
                                    DefaultRecord.Builder.class,
                                    b1 -> b1.setMetrics(
                                            entry.getValue().entrySet().stream().collect(
                                                    ImmutableMap.toImmutableMap(
                                                            Map.Entry::getKey,
                                                            e -> ThreadLocalBuilder.clone(
                                                                    e.getValue(),
                                                                    DefaultMetric.Builder.class))))
                                            .setId(record.getId())
                                            .setTime(record.getTime())
                                            .setAnnotations(record.getAnnotations())
                                            .setDimensions(entry.getKey().getParameters())));
                }
            }
        }

        private Key getModifiedDimensions(
                final ImmutableMap<String, String> inputDimensions,
                final Map<String, String> add,
                final List<String> remove,
                final TransformationSet transformation) {
            final Map<String, String> finalTags = Maps.newHashMap(inputDimensions);
            // Remove the dimensions that we consumed in the replacement
            remove.forEach(finalTags::remove);
            transformation.getRemove().forEach(finalTags::remove);
            transformation.getInject().forEach(
                    (key, inject) ->
                            finalTags.compute(key, (k, oldValue) ->
                                    inject.isOverwriteExisting() || oldValue == null ? inject.getValue() : oldValue));
            finalTags.putAll(add);

            return new DefaultKey(ImmutableMap.copyOf(finalTags));
        }

        private void merge(
                final Metric metric,
                final String key,
                final Map<Key, Map<String, MergingMetric>> mergedMetrics,
                final Key dimensionKey) {

            final Map<String, MergingMetric> mergedMetricsForDimensions =
                    mergedMetrics.computeIfAbsent(dimensionKey, k -> Maps.newHashMap());
            final MergingMetric mergedMetric = mergedMetricsForDimensions.get(key);
            if (mergedMetric == null) {
                // This is the first time this metric is being merged into
                mergedMetricsForDimensions.put(key, new MergingMetric(metric));
            } else if (!mergedMetric.isMergable(metric)) {
                // This instance of the metric is not mergable with previous
                LOGGER.error()
                        .setMessage("Discarding metric")
                        .addData("reason", "failed to merge")
                        .addData("metric", metric)
                        .addData("mergedMetric", mergedMetric)
                        .log();
            } else {
                // Merge the new instance in
                mergedMetric.merge(metric);
            }
        }

        private final TransformingSource _source;
        private final ImmutableList<TransformationSet> _transformations;
    }

    // NOTE: Package private for testing
    /* package private */ static final class MergingMetric implements Metric {

        /* package private */ MergingMetric(final Metric metric) {
            _type = metric.getType();
            _values.addAll(metric.getValues());
        }

        public boolean isMergable(final Metric metric) {
            return _type.equals(metric.getType());
        }

        public void merge(final Metric metric) {
            if (!isMergable(metric)) {
                throw new IllegalArgumentException(String.format("Metric cannot be merged; metric=%s", metric));
            }
            _values.addAll(metric.getValues());
        }

        @Override
        public MetricType getType() {
            return _type;
        }

        @Override
        public ImmutableList<Quantity> getValues() {
            return _values.build();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", Integer.toHexString(System.identityHashCode(this)))
                    .add("Type", _type)
                    .add("Values", _values)
                    .toString();
        }

        private final MetricType _type;
        private final ImmutableList.Builder<Quantity> _values = ImmutableList.builder();
    }

    /**
     * Represents a dimension to inject and whether or not it should overwrite the existing value (if any).
     */
    public static final class DimensionInjection {
        public String getValue() {
            return _value;
        }

        public boolean isOverwriteExisting() {
            return _overwriteExisting;
        }

        private DimensionInjection(final Builder builder) {
            _value = builder._value;
            _overwriteExisting = builder._overwriteExisting;
        }

        private final String _value;
        private final boolean _overwriteExisting;

        /**
         * Implementation of the <code>Builder</code> pattern for {@link DimensionInjection}.
         *
         * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
         */
        public static final class Builder extends OvalBuilder<DimensionInjection> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(DimensionInjection::new);
            }

            /**
             * Sets the value. Required. Cannot be null. Cannot be empty.
             *
             * @param value The value to inject.
             * @return This instance of {@link Builder}.
             */
            public Builder setValue(final String value) {
                _value = value;
                return this;
            }

            /**
             * Whether to override existing dimension of this name. Optional. Cannot be null. Defaults to true.
             *
             * @param value true to replace existing dimension value
             * @return This instance of {@link Builder}.
             */
            public Builder setOverwriteExisting(final Boolean value) {
                _overwriteExisting = value;
                return this;
            }

            @NotNull
            @NotEmpty
            private String _value;
            @NotNull
            private Boolean _overwriteExisting = true;
        }
    }

    /**
     * Represents a set of transformations to apply.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class TransformationSet {
        public ImmutableMap<Pattern, ImmutableList<String>> getFindAndReplace() {
            return _findAndReplace;
        }

        public ImmutableMap<String, DimensionInjection> getInject() {
            return _inject;
        }

        public ImmutableList<String> getRemove() {
            return _remove;
        }

        private TransformationSet(final Builder builder) {
            final ImmutableMap.Builder<Pattern, ImmutableList<String>> findReplaceBuilder =
                    ImmutableMap.builderWithExpectedSize(builder._findAndReplace.size());
            for (final ImmutableMap.Entry<String, ? extends ImmutableList<String>> entry : builder._findAndReplace.entrySet()) {
                findReplaceBuilder.put(Pattern.compile(entry.getKey()), ImmutableList.copyOf(entry.getValue()));
            }
            _findAndReplace = findReplaceBuilder.build();
            _inject = builder._inject;
            _remove = builder._remove;
        }

        private final ImmutableMap<String, DimensionInjection> _inject;
        private final ImmutableList<String> _remove;
        private final ImmutableMap<Pattern, ImmutableList<String>> _findAndReplace;

        /**
         * Implementation of the builder pattern for a {@link TransformationSet}.
         */
        public static final class Builder extends OvalBuilder<TransformationSet> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(TransformationSet::new);
            }

            /**
             * Sets find and replace expression map. Optional. Cannot be null. Defaults to empty.
             *
             * @param value The find and replace expression map.
             * @return This instance of <code>Builder</code>.
             */
            public Builder setFindAndReplace(final ImmutableMap<String, ? extends ImmutableList<String>> value) {
                _findAndReplace = value;
                return this;
            }

            /**
             * Sets dimensions to inject. Optional. Cannot be null. Defaults to empty.
             *
             * @param value List of dimensions to inject.
             * @return This instance of <code>Builder</code>.
             */
            public Builder setInject(final ImmutableMap<String, DimensionInjection> value) {
                _inject = value;
                return this;
            }

            /**
             * Sets dimensions to remove. Optional. Cannot be null. Defaults to empty.
             *
             * @param value List of dimensions to inject.
             * @return This instance of <code>Builder</code>.
             */
            public Builder setRemove(final ImmutableList<String> value) {
                _remove = value;
                return this;
            }

            @NotNull
            private ImmutableMap<String, ? extends ImmutableList<String>> _findAndReplace = ImmutableMap.of();
            @NotNull
            private ImmutableMap<String, DimensionInjection> _inject = ImmutableMap.of();
            @NotNull
            private ImmutableList<String> _remove = ImmutableList.of();
        }
    }

    /**
     * Implementation of builder pattern for {@link TransformingSource}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSource.Builder<Builder, TransformingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TransformingSource::new);
        }

        /**
         * Sets the underlying source. Cannot be null.
         *
         * @param value The underlying source.
         * @return This instance of {@link Builder}.
         */
        public Builder setSource(final Source value) {
            _source = value;
            return this;
        }

        /**
         * Sets the transformations. Required. Cannot be null. Cannot be empty.
         *
         * @param value The list of transformations to apply.
         * @return This instance of {@link Builder}.
         */
        public Builder setTransformations(final ImmutableList<TransformationSet> value) {
            _transformations = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Source _source;
        @NotNull
        @NotEmpty
        private ImmutableList<TransformationSet> _transformations;
    }
}
