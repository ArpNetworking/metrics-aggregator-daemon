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

import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/**
 * Represents the initial position for a tailer.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public enum InitialPosition {
    /**
     * Start at the beginning of a file.
     */
    START {
        @Override
        public long get(final SeekableByteChannel channel) {
            return 0L;
        }
    },
    /**
     * Start at the end of a file.
     */
    END {
        @Override
        public long get(final SeekableByteChannel channel) {
            try {
                return channel.size();
            } catch (final IOException exception) {
                LOGGER.warn()
                        .setMessage("Unable to see to end of byte channel")
                        .addData("channel", channel)
                        .setThrowable(exception)
                        .log();
                return 0;
            }
        }
    };

    InitialPosition() { }

    /**
     * Given a <code>SeekableByteChannel</code>, returns the position in that stream to start from.
     *
     * @param channel Channel to evaluate the position against
     * @return The position (in bytes) to start reading
     */
    public abstract long get(SeekableByteChannel channel);

    private static final Logger LOGGER = LoggerFactory.getLogger(InitialPosition.class);
}
