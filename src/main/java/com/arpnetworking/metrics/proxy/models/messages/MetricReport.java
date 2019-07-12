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
import com.arpnetworking.metrics.mad.model.Unit;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * Message class to hold data about a metric that should be sent to clients.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@Loggable
public final class MetricReport {

    /**
     * Public constructor.
     *
     * @param service name of the service
     * @param host name of the host
     * @param statistic name of the statistic
     * @param metric name of the metric
     * @param value value
     * @param unit unit
     * @param periodStart start of the period
     */
    public MetricReport(
            final String service,
            final String host,
            final String statistic,
            final String metric,
            final double value,
            final Optional<Unit> unit,
            final ZonedDateTime periodStart) {
        _service = service;
        _host = host;
        _statistic = statistic;
        _metric = metric;
        _value = value;
        _numeratorUnits = unit.map(Unit::toString).map(ImmutableList::of).orElse(ImmutableList.of());
        _periodStart = periodStart;
    }

    public String getService() {
        return _service;
    }

    public String getHost() {
        return _host;
    }

    public String getStatistic() {
        return _statistic;
    }

    public String getMetric() {
        return _metric;
    }

    public double getValue() {
        return _value;
    }

    public ImmutableList<String> getNumeratorUnits() {
        return _numeratorUnits;
    }

    public ImmutableList<String> getDenominatorUnits() {
        return _denominatorUnits;
    }

    public ZonedDateTime getPeriodStart() {
        return _periodStart;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("class", this.getClass())
                .add("Service", _service)
                .add("Host", _host)
                .add("Statistic", _statistic)
                .add("Metric", _metric)
                .add("Value", _value)
                .add("NumeratorUnit", _numeratorUnits)
                .add("DenominatorUnit", _denominatorUnits)
                .add("PeriodStart", _periodStart)
                .toString();
    }

    private final String _service;
    private final String _host;
    private final String _statistic;
    private final String _metric;
    private final double _value;
    private final ImmutableList<String> _numeratorUnits;
    private final ImmutableList<String> _denominatorUnits = ImmutableList.of();
    private final ZonedDateTime _periodStart;
}
