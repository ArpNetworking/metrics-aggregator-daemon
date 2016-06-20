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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;

import java.util.Collection;

/**
 * A publisher that wraps multiple others and publishes to all of them. This
 * class is thread safe.
 *
 * TODO(vkoskela): Support concurrent execution [MAI-98]
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public final class MultiSink extends BaseSink {

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", periodicData.getData().size())
                .addData("conditionsSize", periodicData.getConditions().size())
                .log();

        for (final Sink sink : _sinks) {
            sink.recordAggregateData(periodicData);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOGGER.info()
                .setMessage("Closing sink")
                .addData("sink", getName())
                .log();
        for (final Sink sink : _sinks) {
            sink.close();
        }
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("sinks", _sinks)
                .build();
    }

    private MultiSink(final Builder builder) {
        super(builder);
        _sinks = builder._sinks;
    }

    private final Collection<Sink> _sinks;

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiSink.class);

    /**
     * Implementation of builder pattern for <code>MultiSink</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, MultiSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(MultiSink::new);
        }

        /**
         * The aggregated data sinks to wrap. Cannot be null.
         *
         * @param value The aggregated data sinks to wrap.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setSinks(final Collection<Sink> value) {
            _sinks = Lists.newArrayList(value);
            return this;
        }

        /**
         * Adds a sink to the list of sinks.
         *
         * @param value A sink.
         * @return This instance of <code>Builder</code>.
         */
        public Builder addSink(final Sink value) {
            if (_sinks == null) {
                _sinks = Lists.newArrayList();
            }
            _sinks.add(value);
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
        private Collection<Sink> _sinks;
    }
}
