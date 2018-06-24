/*
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.metrics.common.tailer;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of <code>PositionStore</code> which stores the read
 * position in a file on local disk. This class is thread-safe per file
 * identifier.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class FilePositionStore implements PositionStore {

    @Override
    public Optional<Long> getPosition(final String identifier) {
        final Descriptor descriptor = _state.get(identifier);
        if (descriptor == null) {
            return Optional.empty();
        }
        return Optional.of(descriptor.getPosition());
    }

    @Override
    public void setPosition(final String identifier, final long position) {
        final Descriptor descriptor = _state.putIfAbsent(
                identifier,
                new Descriptor.Builder()
                        .setPosition(position)
                        .build());

        final ZonedDateTime now = ZonedDateTime.now();
        boolean requiresFlush = now.minus(_flushInterval).isAfter(_lastFlush);
        if (descriptor != null) {
            descriptor.update(position, now);
            requiresFlush = requiresFlush || descriptor.getDelta() > _flushThreshold;
        }
        if (requiresFlush) {
            flush();
        }
    }

    @Override
    public void close() {
        flush();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.<String, Object>builder()
                .put("file", _file)
                .put("flushInterval", _flushInterval)
                .put("flushThreshold", _flushThreshold)
                .put("retention", _retention)
                .put("lastFlush", _lastFlush)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void flush() {
        // Age out old state
        final ZonedDateTime now = ZonedDateTime.now();
        final ZonedDateTime oldest = now.minus(_retention);
        final long sizeBefore = _state.size();
        final Iterator<Map.Entry<String, Descriptor>> iterator = _state.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, Descriptor> entry = iterator.next();
            if (!oldest.isBefore(entry.getValue().getLastUpdated())) {
                // Remove old descriptors
                iterator.remove();
            } else {
                // Mark retained descriptors as flushed
                entry.getValue().flush();
            }
        }
        final long sizeAfter = _state.size();
        if (sizeBefore != sizeAfter) {
            LOGGER.debug()
                    .setMessage("Removed old entries from file position store")
                    .addData("sizeBefore", sizeBefore)
                    .addData("sizeAfter", sizeAfter)
                    .log();
        }

        // Persist the state to disk
        try {
            final Path temporaryFile = Paths.get(_file.toAbsolutePath().toString() + ".tmp");
            OBJECT_MAPPER.writeValue(temporaryFile.toFile(), _state);
            Files.move(temporaryFile, _file, StandardCopyOption.REPLACE_EXISTING);

            LOGGER.debug()
                    .setMessage("Persisted file position state to disk")
                    .addData("size", _state.size())
                    .addData("file", _file)
                    .log();
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            _lastFlush = now;
        }
    }

    private FilePositionStore(final Builder builder) {
        _file = builder._file;
        _flushInterval = builder._flushInterval;
        _flushThreshold = builder._flushThreshold;
        _retention = builder._retention;

        ConcurrentMap<String, Descriptor> state = Maps.newConcurrentMap();
        try {
            state = OBJECT_MAPPER.readValue(_file.toFile(), STATE_MAP_TYPE_REFERENCE);
        } catch (final IOException e) {
            LOGGER.warn()
                    .setMessage("Unable to load state")
                    .addData("file", _file)
                    .setThrowable(e)
                    .log();
        }
        _state = state;
    }

    private final Path _file;
    private final Duration _flushInterval;
    private final long _flushThreshold;
    private final Duration _retention;
    private final ConcurrentMap<String, Descriptor> _state;

    private ZonedDateTime _lastFlush = ZonedDateTime.now();

    private static final TypeReference<ConcurrentMap<String, Descriptor>> STATE_MAP_TYPE_REFERENCE =
            new TypeReference<ConcurrentMap<String, Descriptor>>(){};
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(FilePositionStore.class);

    private static final class Descriptor {

        public void update(final long position, final ZonedDateTime updatedAt) {
            _delta += position - _position;
            _lastUpdated = updatedAt;
            _position = position;
        }

        public void flush() {
            _delta = 0;
        }

        public long getPosition() {
            return _position;
        }

        public ZonedDateTime getLastUpdated() {
            return _lastUpdated;
        }

        @JsonIgnore
        public long getDelta() {
            return _delta;
        }

        private Descriptor(final Builder builder) {
            _position = builder._position;
            _lastUpdated = builder._lastUpdated;
            _delta = 0;
        }

        private long _position;
        private ZonedDateTime _lastUpdated;
        private long _delta;

        private static final class Builder extends OvalBuilder<Descriptor> {

            private Builder() {
                super(Descriptor::new);
            }

            public Builder setPosition(final Long value) {
                _position = value;
                return this;
            }

            public Builder setLastUpdated(final ZonedDateTime value) {
                _lastUpdated = value;
                return this;
            }

            @NotNull
            private Long _position;
            @NotNull
            private ZonedDateTime _lastUpdated = ZonedDateTime.now();
        }
    }

    /**
     * Implementation of builder pattern for <code>FilePositionStore</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static class Builder extends OvalBuilder<FilePositionStore> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(FilePositionStore::new);
        }

        /**
         * Sets the file to store position in. Cannot be null or empty.
         *
         * @param value The file to store position in.
         * @return This instance of {@link Builder}
         */
        public Builder setFile(final Path value) {
            _file = value;
            return this;
        }

        /**
         * Sets the interval between flushes to the position store. Optional.
         * Default is one minute.
         *
         * @param value The interval between flushes to the position store.
         * @return This instance of {@link Builder}
         */
        public Builder setFlushInterval(final Duration value) {
            _flushInterval = value;
            return this;
        }

        /**
         * Sets the minimum position delta threshold to initiate a flush of the
         * position store. Optional. Default is 1Mb (1024 * 1024 bytes).
         *
         * @param value The minimum position delta threshold.
         * @return This instance of {@link Builder}
         */
        public Builder setFlushThreshold(final Long value) {
            _flushThreshold = value;
            return this;
        }

        /**
         * Sets the duration of an entry in the position store. Optional.
         * Default is one day.
         *
         * @param value The retention of an entry in the position store.
         * @return This instance of {@link Builder}
         */
        public Builder setRetention(final Duration value) {
            _retention = value;
            return this;
        }

        @NotNull
        private Path _file;
        @NotNull
        private Duration _flushInterval = Duration.ofSeconds(10);
        @NotNull
        @Min(0)
        private Long _flushThreshold = 10485760L; // 2^20 * 10 = (10 Mebibyte)
        @NotNull
        private Duration _retention = Duration.ofDays(1);
    }
}
