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

package com.inscopemetrics.mad.telemetry.models.messages;

import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;

/**
 * Message class to inform clients of a new metric.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class NewMetric {
    /**
     * Public constructor.
     *
     * @param service service the metric is from
     * @param metric metric name
     * @param statistic statistic
     */
    public NewMetric(final String service, final String metric, final String statistic) {
        _service = service;
        _metric = metric;
        _statistic = statistic;
    }

    public String getService() {
        return _service;
    }

    public String getMetric() {
        return _metric;
    }

    public String getStatistic() {
        return _statistic;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("Service", _service)
                .add("Metric", _metric)
                .add("Statistic", _statistic)
                .toString();
    }

    private final String _service;
    private final String _metric;
    private final String _statistic;
}
