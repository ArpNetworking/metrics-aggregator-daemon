/*
 * Copyright 2022 Brandon Arp
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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.common.sources.HttpSource;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.util.AbstractMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A publisher that looks for certain metrics and reports on their occurrences.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class TracingSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", periodicData.getData().size())
                .log();
        final ImmutableSet<String> metricNames = periodicData.getData().keySet();
        for (Map.Entry<String, Pattern> entry : _patterns.entrySet()) {
            int hits = 0;
            final Pattern p = entry.getValue();
            for (final String metric : metricNames) {
                if (p.matcher(metric).matches()) {
                    hits++;
                }
            }

            if (hits > 0) {
                try (Metrics metrics = _metricsFactory.create()) {
                    metrics.addAnnotation("sink", getName());
                    metrics.addAnnotation("pattern", entry.getKey());
                    metrics.incrementCounter("tracingSink/hit", hits);
                }
            }
        }

    }

    @Override
    public void close() {
        LOGGER.info()
                .setMessage("Closing sink")
                .addData("sink", getName())
                .log();
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("patterns", _patterns)
                .build();
    }

    private TracingSink(final Builder builder) {
        super(builder);
        _patterns = builder._patterns
                .entrySet()
                .stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), Pattern.compile(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        _metricsFactory = builder._metricsFactory;
    }

    private final Map<String, Pattern> _patterns;
    private final MetricsFactory _metricsFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingSink.class);

    /**
     * Implementation of builder pattern for {@link TracingSink}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
     */
    public static final class Builder extends BaseSink.Builder<Builder, TracingSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TracingSink::new);
        }

        /**
         * The patterns to look for and report on.
         *
         * @param value The aggregated data sinks to wrap.
         * @return This instance of {@link Builder}.
         */
        public Builder setPatterns(final Map<String, String> value) {
            _patterns = Maps.newHashMap(value);
            return this;
        }

        /**
         * Sets the periodic metrics instance.
         *
         * @param value The periodic metrics.
         * @return This instance of {@link HttpSource.Builder}
         */
        public Builder setMetricsFactory(final MetricsFactory value) {
            _metricsFactory = value;
            return this;
        }



            @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Map<String, String> _patterns;

        @NotNull
        @JacksonInject
        private MetricsFactory _metricsFactory;
    }
}
