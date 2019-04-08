/*
 * Copyright 2017 Inscope Metrics, Inc.
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
package io.inscopemetrics.mad.model.telegraf;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Timestamp unit for TelegrafJson JSON data.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public enum TimestampUnit {
    /**
     * TelegrafJson JSON timestamp in seconds.
     */
    SECONDS {
        @Override
        public ZonedDateTime create(final long timestamp) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp * 1000), ZoneOffset.UTC);
        }
    },
    /**
     * TelegrafJson JSON timestamp in milliseconds.
     */
    MILLISECONDS {
        @Override
        public ZonedDateTime create(final long timestamp) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
        }
    },
    /**
     * TelegrafJson JSON timestamp in microseconds.
     */
    MICROSECONDS {
        @Override
        public ZonedDateTime create(final long timestamp) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp / 1000), ZoneOffset.UTC);
        }
    },
    /**
     * TelegrafJson JSON timestamp in nanoseconds.
     */
    NANOSECONDS {
        @Override
        public ZonedDateTime create(final long timestamp) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp / 1000000), ZoneOffset.UTC);
        }
    };

    /**
     * Convert a {@code long} epoch in this unit into a {@code DateTime}.
     *
     * @param timestamp the {@code long} epoch in this unit
     * @return instance of {@code DateTime}
     */
    public abstract ZonedDateTime create(long timestamp);
}
