/**
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

package com.arpnetworking.metrics.proxy.parsers;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.proxy.models.messages.LogLine;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;

import java.nio.file.Path;

/**
 * Represents raw log lines read from a file.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class LogLineParser implements Parser<LogLine> {

    /**
     * Public constructor.
     *
     * @param logFile File the parser is attached to.
     */
    public LogLineParser(final Path logFile) {
        _logFile = logFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LogLine parse(final byte[] data) throws IllegalArgumentException {
        if (null == data) {
            LOGGER.error().setMessage("Null data sent to the FilesSource Parser").log();
            return null;
        }

        return new LogLine(_logFile, data);
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("logFile", _logFile)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private final Path _logFile;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogLineParser.class);
}
