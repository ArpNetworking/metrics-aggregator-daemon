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
package com.arpnetworking.metrics.mad.performance;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.configuration.jackson.JsonNodeLiteralSource;
import com.arpnetworking.configuration.jackson.StaticConfiguration;
import com.arpnetworking.metrics.generator.util.TestFileGenerator;
import com.arpnetworking.metrics.mad.Pipeline;
import com.arpnetworking.metrics.mad.configuration.PipelineConfiguration;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Serves as the base for performance tests that run a file through a tsd aggregator pipeline.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class FilePerfTestBase {

    /**
     * Runs a filter.
     *
     * @param pipelineConfigurationFile Pipeline configuration file.
     * @param duration Timeout period.
     * @param variables Substitution key-value pairs into pipeline configuration file.
     * @throws IOException if configuration cannot be loaded.
     */
    protected void benchmark(
            final String pipelineConfigurationFile,
            final Duration duration,
            final ImmutableMap<String, String> variables)
            throws IOException {
        // Replace any variables in the configuration file
        String configuration = Resources.toString(Resources.getResource(pipelineConfigurationFile), StandardCharsets.UTF_8);
        for (final Map.Entry<String, String> entry : variables.entrySet()) {
            configuration = configuration.replace(entry.getKey(), entry.getValue());
        }

        // Load the specified stock configuration
        final PipelineConfiguration stockPipelineConfiguration = new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder()
                        .setSource(configuration)
                        .build())
                .setObjectMapper(PipelineConfiguration.createObjectMapper(_injector))
                .build()
                .getRequiredAs(PipelineConfiguration.class);

        // Canary tracking
        LOGGER.info(String.format(
                "Expected canaries; periods=%s",
                stockPipelineConfiguration.getPeriods()));
        final CountDownLatch latch = new CountDownLatch(stockPipelineConfiguration.getPeriods().size());
        final Set<Duration> periods = Sets.newConcurrentHashSet();

        // Create custom "canary" sink
        final ListeningSink sink = new ListeningSink(periodicData -> {
                if (periodicData != null) {
                    for (final String metricName : periodicData.getData().keys()) {
                        if (TestFileGenerator.CANARY.equals(metricName)) {
                            if (periods.add(periodicData.getPeriod())) {
                                LOGGER.info(String.format(
                                        "Canary flew; filter=%s, period=%s",
                                        this.getClass(),
                                        periodicData.getPeriod()));
                                latch.countDown();
                            }
                        }
                    }
                }
                return null;
            }
        );

        // Add the custom "canary" sink
        final List<Sink> benchmarkSinks = Lists.newArrayList(stockPipelineConfiguration.getSinks());
        benchmarkSinks.add(sink);

        // Create the custom configuration
        final PipelineConfiguration benchmarkPipelineConfiguration =
                OvalBuilder.<PipelineConfiguration, PipelineConfiguration.Builder>clone(stockPipelineConfiguration)
                        .setSinks(benchmarkSinks)
                        .build();

        // Instantiate the pipeline
        final Pipeline pipeline = new Pipeline(benchmarkPipelineConfiguration);

        // Execute the pipeline until the canary flies the coop
        try {
            LOGGER.debug(String.format("Launching pipeline; configuration=%s", pipelineConfigurationFile));
            final Stopwatch timer = Stopwatch.createUnstarted();
            timer.start();
            pipeline.launch();

            if (!latch.await(duration.toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.error("Test timed out");
                throw new RuntimeException("Test timed out");
            }

            timer.stop();
            LOGGER.info(String.format(
                    "Performance filter result; filter=%s, seconds=%s",
                    this.getClass(),
                    timer.elapsed(TimeUnit.SECONDS)));

        } catch (final InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException("Test interrupted");
        } finally {
            pipeline.shutdown();
        }
    }

    private final Injector _injector = Guice.createInjector();

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePerfTestBase.class);
}
