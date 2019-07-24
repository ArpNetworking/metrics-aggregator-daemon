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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Implementation of {@code PeriodicMetrics} that collects the metrics it records.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class CollectorPeriodicMetrics implements PeriodicMetrics, Runnable {
    private final Map<String, List<Long>> _counts = new ConcurrentHashMap<>();
    private final Map<String, List<Number>> _gauges = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> _timers = new ConcurrentHashMap<>();

    @Override
    public void registerPolledMetric(final java.util.function.Consumer<PeriodicMetrics> consumer) {
        _polledMetricsRegistrations.add(consumer);
    }

    @Override
    public void recordCounter(final String name, final long value) {
        _counts.computeIfAbsent(name, (String key) -> new ArrayList<>());
        _counts.get(name).add(value);
    }

    @Override
    public void recordGauge(final String name, final long value) {
        _gauges.computeIfAbsent(name, (String key) -> new ArrayList<>());
        _gauges.get(name).add(value);
    }

    @Override
    public void recordGauge(final String name, final long value, final Optional<Unit> unit) {
        _gauges.computeIfAbsent(name, (String key) -> new ArrayList<>());
        _gauges.get(name).add(value);
    }

    @Override
    public void recordGauge(final String name, final double value) {
        _gauges.computeIfAbsent(name, (String key) -> new ArrayList<>());
        _gauges.get(name).add(value);
    }

    @Override
    public void recordGauge(final String name, final double value, final Optional<Unit> unit) {
        _gauges.computeIfAbsent(name, (String key) -> new ArrayList<>());
        _gauges.get(name).add(value);
    }

    @Override
    public void recordTimer(final String name, final long duration, final Optional<Unit> unit) {
        _timers.computeIfAbsent(name, (String key) -> new ArrayList<>());
        _timers.get(name).add(duration);
    }

    private final Set<Consumer<PeriodicMetrics>> _polledMetricsRegistrations = ConcurrentHashMap.newKeySet();

    @Override
    public void run() {
        for (java.util.function.Consumer<PeriodicMetrics> polledMetric : _polledMetricsRegistrations) {
            polledMetric.accept(this);
        }
    }

    /**
     * Returns the list of collected counters for a specific metric.
     *
     * @param name the name of the metric
     * @return the list of counters
     */
    public List<Long> getCounters(final String name) {
        return _counts.get(name);
    }

    /**
     * Returns the list of collected gauges for a specific metric.
     *
     * @param name the name of the metric
     * @return the list of gauges
     */
    public List<Number> getGauges(final String name) {
        return _gauges.get(name);
    }

    /**
     * Returns the list of collected timers for a specific metric.
     *
     * @param name the name of the metric
     * @return the list of timers
     */
    public List<Long> getTimers(final String name) {
        return _timers.get(name);
    }
}
