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

package io.inscopemetrics.mad.telemetry.models.messages;

import com.arpnetworking.logback.annotations.Loggable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

/**
 * Message class to hold a tree of metrics.
 *
 * NOTE: This is not marked as @Loggable since it only has one attribute which we
 * do not wish to log with it since it contains a lot of data.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@Loggable
public final class MetricsList {
    /**
     * Public constructor.
     *
     * @param metrics Metrics map
     */
    public MetricsList(final ImmutableMap<String, Map<String, Set<String>>> metrics) {
        _metrics = metrics;
    }

    // TODO(vkoskela): Switch to @LogIgnore once available in Logback-Steno [ISSUE-3]
    @JsonIgnore
    public Map<String, Map<String, Set<String>>> getMetrics() {
        return _metrics;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("Metrics", _metrics)
                .toString();
    }

    private final ImmutableMap<String, Map<String, Set<String>>> _metrics;
}
