/**
 * Copyright 2018 Inscope Metrics, Inc
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

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.common.sources.BaseSource;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.util.Map;

/**
 * Implementation of <code>Source</code> which wraps another <code>Source</code>
 * and injects (and optionally overwrites) dimensions.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class DimensionInjectingSource extends BaseSource {

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
                .put("dimensions", _dimensions)
                .put("replaceExisting", _replaceExisting)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private DimensionInjectingSource(final Builder builder) {
        super(builder);
        _source = builder._source;
        _replaceExisting = builder._replaceExisting;
        _dimensions = ImmutableMap.<String, String>builder().putAll(builder._dimensions).build();

        _source.attach(new InjectingObserver(this, _replaceExisting, _dimensions));
    }

    private final Source _source;
    private final boolean _replaceExisting;
    private final ImmutableMap<String, String> _dimensions;

    private static final Logger LOGGER = LoggerFactory.getLogger(DimensionInjectingSource.class);

    // NOTE: Package private for testing
    /* package private */ static final class InjectingObserver implements Observer {

        /* package private */ InjectingObserver(
                final DimensionInjectingSource source,
                final boolean replace,
                final ImmutableMap<String, String> dimensions) {
            _source = source;
            _replace = replace;
            _dimensions = dimensions;
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
            final Map<String, String> newDimensions;
            if (_replace) {
                newDimensions = Maps .newHashMapWithExpectedSize(record.getAnnotations().size() + _dimensions.size());
                newDimensions.putAll(record.getDimensions());
                newDimensions.putAll(_dimensions);
            } else {
                newDimensions = Maps .newHashMapWithExpectedSize(record.getAnnotations().size() + _dimensions.size());
                newDimensions.putAll(_dimensions);
                newDimensions.putAll(record.getDimensions());
            }

            _source.notify(ThreadLocalBuilder.build(
                    DefaultRecord.Builder.class,
                    b1 -> b1.setMetrics(record.getMetrics())
                            .setId(record.getId())
                            .setTime(record.getTime())
                            .setAnnotations(record.getAnnotations())
                            .setDimensions(ImmutableMap.copyOf(newDimensions))));
        }

        private final DimensionInjectingSource _source;
        private final boolean _replace;
        private final ImmutableMap<String, String> _dimensions;
    }

    /**
     * Implementation of builder pattern for {@link DimensionInjectingSource}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSource.Builder<Builder, DimensionInjectingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DimensionInjectingSource::new);
        }

        /**
         * Sets the underlying source. Required. Cannot be null.
         *
         * @param value The underlying source.
         * @return This instance of {@link Builder}.
         */
        public Builder setSource(final Source value) {
            _source = value;
            return this;
        }

        /**
         * Sets the dimensions. Required. Cannot be null.
         *
         * @param value The map of dimensions and their values
         * @return This instance of {@link Builder}.
         */
        public Builder setDimensions(final Map<String, String> value) {
            _dimensions = value;
            return this;
        }

        /**
         * Sets whether or not to replace existing values. Optional. Cannot be null. Defaults to true.
         *
         * @param value True to replace values
         * @return This instance of {@link Builder}.
         */
        public Builder setReplaceExisting(final Boolean value) {
            _replaceExisting = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Source _source;
        @NotNull
        private Map<String, String> _dimensions;
        @NotNull
        private Boolean _replaceExisting = true;
    }
}
