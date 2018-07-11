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
package com.inscopemetrics.mad.model.statsd;

import com.google.common.collect.Maps;
import com.inscopemetrics.mad.model.MetricType;
import com.inscopemetrics.mad.model.Unit;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * The statsd data type.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public enum StatsdType {
    /**
     * The counter statsd metric type.
     */
    COUNTER("c", MetricType.COUNTER, null),
    /**
     * The gauge statsd metric type.
     */
    GAUGE("g", MetricType.GAUGE, null),
    /**
     * The histogram statsd metric type.
     */
    HISTOGRAM("h", MetricType.TIMER, null),
    /**
     * The meter statsd metric type.
     */
    METERS("m", MetricType.COUNTER, null),
    // NOTE: Sets are not supported as per class Javadoc.
    //SET("s", null),
    /**
     * The timer statsd metric type.
     */
    TIMER("ms", MetricType.TIMER, Unit.MILLISECOND);

    private final String _token;
    private final MetricType _metricType;
    private @Nullable final Unit _unit;

    private static final Map<String, StatsdType> TOKEN_TO_TYPE = Maps.newHashMap();

    StatsdType(
            final String token,
            final MetricType metricType,
            @Nullable final Unit unit) {
        _token = token;
        _metricType = metricType;
        _unit = unit;
    }

    public MetricType getMetricType() {
        return _metricType;
    }

    public @Nullable Unit getUnit() {
        return _unit;
    }

    /**
     * Look-up a {@link StatsdType} by its stats protocol token.
     *
     * @param token the statsd protocol token
     * @return the corresponding {@link StatsdType} or {@code null}
     */
    public static @Nullable StatsdType fromToken(final String token) {
        return TOKEN_TO_TYPE.get(token);
    }

    static {
        for (final StatsdType statsdType : values()) {
            TOKEN_TO_TYPE.put(statsdType._token, statsdType);
        }
    }
}
