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

package com.arpnetworking.metrics.proxy.models.messages;

import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.nio.file.Path;
import java.util.Set;

/**
 * Message class to hold a list of logs.
 *
 * @author Mohammed Kamel (mkamel at groupon dot com)
 */
@Loggable
public final class LogsList {
    /**
     * Public constructor.
     *
     * @param logs Paths of logs
     */
    public LogsList(final Set<Path> logs) {
        _logs = ImmutableSet.copyOf(logs);
    }

    public Set<Path> getLogs() {
        return _logs;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("Logs", _logs)
                .toString();
    }

    private final Set<Path> _logs;
}
