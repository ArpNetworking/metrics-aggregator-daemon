/*
 * Copyright 2019 Dropbox.com
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
package com.arpnetworking.test;

import com.arpnetworking.metrics.Unit;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Implementation of {@code PeriodicMetrics} that collects the metrics it records.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class CollectingPeriodicMetrics implements PeriodicMetrics, Runnable {
    private List<Long> _counters = Collections.synchronizedList(new ArrayList<>());
    private List<Long> _gauges = Collections.synchronizedList(new ArrayList<>());
    private List<Long> _timers = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void registerPolledMetric(final java.util.function.Consumer<PeriodicMetrics> consumer) {
        _polledMetricsRegistrations.add(consumer);
    }

    @Override
    public void recordCounter(final String name, final long value) {
        _counters.add(value);
    }

    @Override
    public void recordGauge(final String name, final long value) {
        _gauges.add(value);
    }

    @Override
    public void recordGauge(final String name, final long value, final Optional<Unit> unit) {
        _gauges.add(value);
    }

    @Override
    public void recordTimer(final String name, final long duration, final Optional<Unit> unit) {
        _timers.add(duration);
    }

    @Override
    public void run() {
        for (java.util.function.Consumer<PeriodicMetrics> polledMetric : _polledMetricsRegistrations) {
            polledMetric.accept(this);
        }
    }

    private final Set<Consumer<PeriodicMetrics>> _polledMetricsRegistrations = ConcurrentHashMap.newKeySet();

    @Override
    public void recordGauge(final String name, final double value) {

    }

    @Override
    public void recordGauge(final String name, final double value, final Optional<Unit> unit) {
    }


    public List<Long> getCounters() {
        return _counters;
    }

    public List<Long> getGauges() {
        return _gauges;
    }

    public List<Long> getTimers() {
        return _timers;
    }
}
