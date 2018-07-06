/*
 * Copyright 2015 Groupon.com
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
package com.inscopemetrics.tsdcore.sinks;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.inscopemetrics.tsdcore.model.AggregatedData;
import com.inscopemetrics.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A sink to filter old data.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TimeThresholdSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", periodicData.getData().size())
                .log();

        if (_logOnly) {
            // Apply the filter but ignore the result
            _filter.filter(periodicData);
            _sink.recordAggregateData(periodicData);
        } else {
            // Apply the filter and rebuild the periodic data
            final ImmutableMultimap<String, AggregatedData> filteredData = _filter.filter(periodicData);
            if (!filteredData.isEmpty()) {
                _sink.recordAggregateData(
                        ThreadLocalBuilder.clone(
                                periodicData,
                                PeriodicData.Builder.class,
                                b -> b.setData(filteredData)));
            }
        }
    }

    @Override
    public void close() {
        _sink.close();
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("excludedServices", _excludedServices)
                .put("sink", _sink)
                .put("logOnly", _logOnly)
                .put("threshold", _threshold)
                .build();
    }

    private TimeThresholdSink(final Builder builder) {
        super(builder);
        _excludedServices = Sets.newConcurrentHashSet(builder._excludedServices);
        _sink = builder._sink;
        _logOnly = builder._logOnly;
        _threshold = builder._threshold;
        _logger = (PeriodicData data) ->
                STALE_DATA_LOGGER
                        .warn()
                        .setMessage("Dropped stale data")
                        .addData("sink", getName())
                        .addData("threshold", _threshold)
                        .addData("data", data)
                        .log();
        _filter = new Filter(_threshold, _logger, _excludedServices);
    }

    private final Consumer<PeriodicData> _logger;
    private final Set<String> _excludedServices;
    private final Sink _sink;
    private final boolean _logOnly;
    private final Duration _threshold;
    private final Filter _filter;
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeThresholdSink.class);
    private static final Logger STALE_DATA_LOGGER = LoggerFactory.getRateLimitLogger(TimeThresholdSink.class, Duration.ofSeconds(30));


    private static final class Filter {
        private Filter(
                final Duration freshnessThreshold,
                final Consumer<PeriodicData> excludedConsumer,
                final Set<String> excludedServices) {
            _freshnessThreshold = freshnessThreshold;
            _excludedConsumer = excludedConsumer;
            _excludedServices = excludedServices;
        }

        public ImmutableMultimap<String, AggregatedData> filter(final PeriodicData periodicData) {
            final ImmutableMultimap.Builder<String, AggregatedData> retainedDataBuilder = ImmutableMultimap.builder();
            if (!periodicData.getStart().plus(periodicData.getPeriod()).plus(_freshnessThreshold).isAfter(ZonedDateTime.now())
                    && !_excludedServices.contains(periodicData.getDimensions().getService())) {
                // Exclude all data
                _excludedConsumer.accept(periodicData);
                return ImmutableMultimap.of();
            }

            // Retain all; either because the data ias not late or the service is excluded
            return periodicData.getData();
        }

        private final Duration _freshnessThreshold;
        private final Consumer<PeriodicData> _excludedConsumer;
        private final Set<String> _excludedServices;
    }

    /**
     * Base <code>Builder</code> implementation.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, TimeThresholdSink> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(TimeThresholdSink::new);
        }

        /**
         * The aggregated data sink to filter. Cannot be null.
         *
         * @param value The aggregated data sink to filter.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * Sets excluded services.  Services in this set will never have their data dropped. Optional.
         * Cannot be null. Default is no excluded services.
         *
         * @param value The excluded services.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setExcludedServices(final Set<String> value) {
            _excludedServices = value;
            return self();
        }

        /**
         * Flag to only log violations instead of dropping data. Optional. Defaults to false.
         *
         * @param value true to log violations, but still pass data
         * @return This instance of <code>Builder</code>.
         */
        public Builder setLogOnly(final Boolean value) {
            _logOnly = value;
            return self();
        }

        /**
         * The freshness threshold to log or drop data. Required. Cannot be null.
         *
         * @param value The threshold for accepted data.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setThreshold(final Duration value) {
            _threshold = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Set<String> _excludedServices = Collections.emptySet();
        @NotNull
        private Sink _sink;
        @NotNull
        private Duration _threshold;
        @NotNull
        private Boolean _logOnly = false;
    }
}
