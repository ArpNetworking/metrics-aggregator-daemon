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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicLongMap;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Implementation of {@code PeriodicMetrics} that logs the metrics it records each time step.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class LoggingPeriodicMetrics implements PeriodicMetrics, Runnable {

    private AtomicReference<AtomicLongMap<String>> _counters = new AtomicReference<>(AtomicLongMap.create());
    private AtomicReference<AtomicLongMap<String>> _gauges = new AtomicReference<>(AtomicLongMap.create());
    private AtomicReference<AtomicLongMap<String>> _timers = new AtomicReference<>(AtomicLongMap.create());

    @Override
    public void registerPolledMetric(final Consumer<PeriodicMetrics> consumer) {
        _polledMetricsRegistrations.add(consumer);
    }

    @Override
    public void recordCounter(final String name, final long value) {
        _counters.get().getAndAdd(name, value);
    }

    @Override
    public void recordGauge(final String name, final long value) {
        _gauges.get().put(name, value);
    }

    @Override
    public void recordGauge(final String name, final long value, final Optional<Unit> unit) {
        _gauges.get().put(name, value);
    }

    @Override
    public void recordTimer(final String name, final long duration, final Optional<Unit> unit) {
        _timers.get().put(name, duration);
    }

    @Override
    public void run() {
        recordPolledMetrics();
        log();
    }

    private void log() {
        final AtomicLongMap<String> counter = _counters.getAndSet(AtomicLongMap.create());
        final AtomicLongMap<String> gauges = _gauges.get();
        final AtomicLongMap<String> timers = _timers.getAndSet(AtomicLongMap.create());

        System.out.printf("Counts\t%s%n", counter);
        System.out.printf("Gauges\t%s%n", gauges);
        System.out.printf("Timers\t%s%n", timers);
    }

    private CompletableFuture<Void> recordPolledMetrics() {
        final List<CompletableFuture<?>> futures = Lists.newArrayList();
        for (Consumer<PeriodicMetrics> polledMetric : _polledMetricsRegistrations) {
            futures.add(CompletableFuture.runAsync(() -> polledMetric.accept(this), _pollingExecutor));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]));
    }

    private final Set<Consumer<PeriodicMetrics>> _polledMetricsRegistrations = ConcurrentHashMap.newKeySet();
    private Executor _pollingExecutor = MoreExecutors.directExecutor();

    @Override
    public void recordGauge(final String name, final double value) {

    }

    @Override
    public void recordGauge(final String name, final double value, final Optional<Unit> unit) {
    }
}
