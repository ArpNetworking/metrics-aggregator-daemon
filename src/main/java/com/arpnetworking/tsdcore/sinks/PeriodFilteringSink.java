/**
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import net.sf.oval.constraint.NotNull;
import org.joda.time.Period;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * A publisher that wraps another, filters the metrics with specific periods,
 * and forwards included metrics to the wrapped sink. This  class is thread
 * safe.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class PeriodFilteringSink extends BaseSink {

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        if (_cachedFilterResult.getUnchecked(periodicData.getPeriod())) {
            _sink.recordAggregateData(periodicData);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        _sink.close();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.<String, Object>builder()
                .put("super", super.toLogValue())
                .put("include", _include)
                .put("exclude", _exclude)
                .put("excludeLessThan", _excludeLessThan)
                .put("excludeGreaterThan", _excludeGreaterThan)
                .put("sink", _sink)
                .build();
    }

    private PeriodFilteringSink(final Builder builder) {
        super(builder);
        _cachedFilterResult = CacheBuilder.newBuilder()
                .maximumSize(10)
                .build(new CacheLoader<Period, Boolean>() {
                    @Override
                    public Boolean load(final Period key) throws Exception {
                        if (_include.contains(key)) {
                            return true;
                        }
                        if (_exclude.contains(key)) {
                            return false;
                        }
                        if (_excludeLessThan.isPresent()
                                && key.toStandardDuration().isShorterThan(_excludeLessThan.get().toStandardDuration())) {
                            return false;
                        }
                        if (_excludeGreaterThan.isPresent()
                                && key.toStandardDuration().isLongerThan(_excludeGreaterThan.get().toStandardDuration())) {
                            return false;
                        }
                        return true;
                    }
                });
        _exclude = Sets.newConcurrentHashSet(builder._exclude);
        _include = Sets.newConcurrentHashSet(builder._include);
        _excludeLessThan = Optional.ofNullable(builder._excludeLessThan);
        _excludeGreaterThan = Optional.ofNullable(builder._excludeGreaterThan);
        _sink = builder._sink;
    }

    private final LoadingCache<Period, Boolean> _cachedFilterResult;
    private final Set<Period> _exclude;
    private final Set<Period> _include;
    private final Optional<Period> _excludeLessThan;
    private final Optional<Period> _excludeGreaterThan;
    private final Sink _sink;

    /**
     * Base <code>Builder</code> implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, PeriodFilteringSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(PeriodFilteringSink::new);
        }

        /**
         * Sets excluded periods. Optional. Default is no excluded periods.
         *
         * @param value The excluded periods.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setExclude(final Set<Period> value) {
            _exclude = value;
            return self();
        }

        /**
         * Sets included periods. Included periods supercede all other
         * settings. Optional. Default is no included periods.
         *
         * @param value The included periods.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setInclude(final Set<Period> value) {
            _include = value;
            return self();
        }

        /**
         * Sets excluded periods less than this period. Optional. Default is no threshold.
         *
         * @param value The excluded period threshold.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setExcludeLessThan(final Period value) {
            _excludeLessThan = value;
            return self();
        }

        /**
         * Sets excluded periods greater than this period. Optional. Default is no threshold.
         *
         * @param value The excluded period threshold.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setExcludeGreaterThan(final Period value) {
            _excludeGreaterThan = value;
            return self();
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
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Set<Period> _exclude = Collections.emptySet();
        @NotNull
        private Set<Period> _include = Collections.emptySet();
        private Period _excludeLessThan;
        private Period _excludeGreaterThan;
        @NotNull
        private Sink _sink;
    }
}
