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

package com.arpnetworking.metrics.proxy.models.messages;

import com.arpnetworking.logback.annotations.Loggable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * Message class to hold data about a log entry that should be sent to clients.
 *
 * @author Mohammed Kamel (mkamel at groupon dot com)
 * @author Vivek Muppala (vivek at groupon dot com)
 */
@Loggable
public final class LogLine {
    /**
     * Public constructor.
     *
     * @param file The name of the log file
     * @param line The raw log line.
     */
    // TODO(vkoskela): Investigate the use of immutable rotating buffers [MAI-489]
    public LogLine(
            final Path file,
            final byte[] line) {
        _file = file;
        _line = line;
    }

    public Path getFile() {
        return _file;
    }

    // TODO(vkoskela): Switch to @LogIgnore once available in Logback-Steno [ISSUE-3]
    @JsonIgnore
    public List<Byte> getLine() {
        return Collections.unmodifiableList(Bytes.asList(_line));
    }

    /**
     * Return the log line as a {@link String}. Callers should cache this
     * value. The data is interpreted as UTF-8 by default.
     *
     * @return The line data as a {@link String} interpreted as UTF-8.
     */
    public String convertLineToString() {
        return convertLineToString(StandardCharsets.UTF_8);
    }

    /**
     * Return the log line as a {@link String}. Callers should cache this
     * value.
     *
     * @param charset The character set to use.
     * @return The line data as a {@link String}.
     */
    public String convertLineToString(final Charset charset) {
        // CHECKSTYLE.OFF: IllegalInstantiation - Unavoidable.
        return new String(_line, charset);
        // CHECKSTYLE.ON: IllegalInstantiation
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("File", _file)
                .add("Line", _line)
                .toString();
    }

    private final Path _file;
    private final byte[] _line;
}
