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

import java.util.Optional;

/**
 * PeriodicMetrics object that No ops whenever called.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class NopPeriodicMetrics implements PeriodicMetrics, Runnable {
    @Override
    public void run() {

    }

    @Override
    public void recordCounter(final String name, final long value) {

    }

    @Override
    public void registerPolledMetric(final java.util.function.Consumer<PeriodicMetrics> consumer) {

    }

    @Override
    public void recordTimer(final String name, final long duration, final Optional<Unit> unit) {

    }

    @Override
    public void recordGauge(final String name, final double value) {

    }

    @Override
    public void recordGauge(final String name, final double value, final Optional<Unit> unit) {

    }

    @Override
    public void recordGauge(final String name, final long value) {

    }

    @Override
    public void recordGauge(final String name, final long value, final Optional<Unit> unit) {

    }
}
