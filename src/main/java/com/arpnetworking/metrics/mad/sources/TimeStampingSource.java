/*
 * Copyright 2019 Dropbox.com
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
import net.sf.oval.constraint.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Implementation of {@link Source} which wraps another {@link Source} and sets
 * the time stamp of the {@link Record}s received from the wrapped {@link Source}
 * to the current time.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class TimeStampingSource extends BaseSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappingSource.class);

    private final Source _source;

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
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private TimeStampingSource(final Builder builder) {
        super(builder);
        _source = builder._source;
        _source.attach(new TimeStampingObserver(this));
    }


    private static final class TimeStampingObserver implements Observer {
        private final TimeStampingSource _source;

        TimeStampingObserver(final TimeStampingSource source) {
            _source = source;
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

            final Record record = (Record) event;
            _source.notify(
                    ThreadLocalBuilder.build(
                            DefaultRecord.Builder.class,
                            b1 -> b1.setMetrics(record.getMetrics())
                                    .setId(record.getId())
                                    .setTime(ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
                                    .setAnnotations(record.getAnnotations())
                                    .setDimensions(record.getDimensions())));
        }
    }

    /**
     * Builder pattern for {@link TimeStampingSource}.
     *
     * @author Joey Jackson (jjackson at dropbox dot com)
     */
    public static final class Builder extends BaseSource.Builder<Builder, TimeStampingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TimeStampingSource::new);
        }

        /**
         * Sets the wrapped {@link Source}. Cannot be null.
         *
         * @param value The wrapped source
         * @return This instance of {@link TimeStampingSource.Builder}
         */
        public Builder setSource(final Source value) {
            _source = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Source _source;
    }
}
