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
package com.inscopemetrics.metrics.mad;

import com.arpnetworking.logback.annotations.LogValue;
import com.inscopemetrics.metrics.common.sources.Source;
import com.inscopemetrics.metrics.mad.configuration.PipelineConfiguration;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.inscopemetrics.tsdcore.sinks.MultiSink;
import com.inscopemetrics.tsdcore.sinks.Sink;
import com.inscopemetrics.utility.Launchable;
import com.google.common.collect.Lists;
import com.inscopemetrics.metrics.mad.configuration.PipelineConfiguration;
import com.inscopemetrics.utility.Launchable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single data pathway through the time series data aggregator. The pathway
 * consists of zero or more sources, typically at least one source is specified,
 * a <code>LineProcessor</code> and zero or more sinks, again typically at least
 * one sink is specified.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class Pipeline implements Launchable {

    /**
     * Public constructor.
     *
     * @param pipelineConfiguration Instance of <code>PipelineConfiguration</code>.
     */
    public Pipeline(final PipelineConfiguration pipelineConfiguration) {
        _pipelineConfiguration = pipelineConfiguration;
    }

    /**
     * Launch the pipeline.
     */
    @Override
    public synchronized void launch() {
        LOGGER.info()
                .setMessage("Launching pipeline")
                .addData("configuration", _pipelineConfiguration)
                .log();

        final Sink rootSink = new MultiSink.Builder()
                .setName(_pipelineConfiguration.getName())
                .setSinks(_pipelineConfiguration.getSinks())
                .build();
        _sinks.add(rootSink);

        final Aggregator aggregator = new Aggregator.Builder()
                .setPeriods(_pipelineConfiguration.getPeriods())
                .setTimerStatistics(_pipelineConfiguration.getTimerStatistics())
                .setCounterStatistics(_pipelineConfiguration.getCounterStatistics())
                .setGaugeStatistics(_pipelineConfiguration.getGaugeStatistics())
                .setStatistics(_pipelineConfiguration.getStatistics())
                .setSink(rootSink)
                .build();
        aggregator.launch();
        _aggregator.set(aggregator);

        for (final Source source : _pipelineConfiguration.getSources()) {
            source.attach(aggregator);
            source.start();
            _sources.add(source);
        }
    }

    /**
     * Shutdown the pipeline.
     */
    @Override
    public synchronized void shutdown() {
        LOGGER.info()
                .setMessage("Stopping pipeline")
                .addData("pipeline", _pipelineConfiguration.getName())
                .log();

        for (final Source source : _sources) {
            source.stop();
        }
        final Optional<Aggregator> aggregator = Optional.ofNullable(_aggregator.getAndSet(null));
        if (aggregator.isPresent()) {
            aggregator.get().shutdown();
        }
        for (final Sink sink : _sinks) {
            sink.close();
        }

        _sources.clear();
        _sinks.clear();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("pipelineConfiguration", _pipelineConfiguration)
                .put("aggregator", _aggregator)
                .put("sinks", _sinks)
                .put("sources", _sources)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private final PipelineConfiguration _pipelineConfiguration;
    private final AtomicReference<Aggregator> _aggregator = new AtomicReference<>();
    private final List<Sink> _sinks = Lists.newArrayList();
    private final List<Source> _sources = Lists.newArrayList();

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);
}
