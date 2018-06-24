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
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Message class to hold data about a log entry that should be sent to clients.
 *
 * @author Mohammed Kamel (mkamel at groupon dot com)
 */
@Loggable
public final class LogReport {

    /**
     * Public constructor.
     *  @param matchingRegexes The regular expression(s) that matched.
     * @param file File the line came from.
     * @param line The line from the log file.
     * @param timestamp Timestamp the line was recorded at.
     */
    public LogReport(
            final List<String> matchingRegexes,
            final Path file,
            final String line,
            final ZonedDateTime timestamp) {
        _file = file;
        _line = line;
        _timestamp = timestamp;
        _matchingRegexes = ImmutableList.copyOf(matchingRegexes);

    }

    public Path getFile() {
        return _file;
    }

    public String getLine() {
        return _line;
    }

    public ZonedDateTime getTimestamp() {
        return _timestamp;
    }

    public List<String> getMatchingRegexes() {
        return _matchingRegexes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("File", _file)
                .add("Line", _line)
                .add("Timestamp", _timestamp)
                .add("MatchingRegexes", _matchingRegexes)
                .toString();
    }

    private final List<String> _matchingRegexes;
    private Path _file;
    private String _line;
    private ZonedDateTime _timestamp;
}
