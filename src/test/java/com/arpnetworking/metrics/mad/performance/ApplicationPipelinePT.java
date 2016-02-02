/**
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

import com.arpnetworking.metrics.generator.util.TestFileGenerator;
import com.arpnetworking.test.junitbenchmarks.JsonBenchmarkConsumer;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Perf tests that cover reading from a file and computing the aggregates
 * from it.
 *
 * @author Brandon Arp (barp at groupon dot com)
 */
@RunWith(Parameterized.class)
@BenchmarkOptions(callgc = true, benchmarkRounds = 1, warmupRounds = 0)
public class ApplicationPipelinePT extends FilePerfTestBase {

    public ApplicationPipelinePT(final String name, final int uowCount, final int namesCount, final int samplesCount) {
        _uowCount = uowCount;
        _namesCount = namesCount;
        _samplesCount = samplesCount;
    }

    @BeforeClass
    public static void setUp() {
        JSON_BENCHMARK_CONSUMER.prepareClass();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> createParameters() {
        final List<Integer> metricSamplesPerUOW = Lists.newArrayList(1, 5, 25);
        final List<Integer> uowPerInterval = Lists.newArrayList(10000, 30000, 90000);
        final List<Integer> metricNamesPerUOW = Lists.newArrayList(1, 10, 100);

        final ArrayList<Object[]> params = Lists.newArrayList();
        for (final Integer uowCount : uowPerInterval) {
            for (final Integer namesCount : metricNamesPerUOW) {
                for (final Integer samplesCount : metricSamplesPerUOW) {
                    final String name = "uow" + uowCount + "-names" + namesCount + "-samples" + samplesCount;
                    params.add(new Object[]{name, uowCount, namesCount, samplesCount});
                }
            }
        }

        return params;
    }

    @Test
    public void test() throws IOException, InterruptedException, URISyntaxException {
        LOGGER.info(String.format(
                "ApplicationPipeline Performance Test; uowCount=%d, namesCount=%d, samplesCount=%d",
                _uowCount, _namesCount, _samplesCount));

        final RandomGenerator random = new MersenneTwister(1298); //Just pick a number as the seed.
        final Path path = Paths.get("target/tmp/perf/application-generated-sample.log");
        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        final DateTime start = DateTime.now().minusDays(1).hourOfDay().roundFloorCopy();
        final DateTime stop = start.plusMinutes(10);
        final TestFileGenerator generator = new TestFileGenerator.Builder()
                .setRandom(random)
                .setUnitOfWorkCount(_uowCount)
                .setNamesCount(_namesCount)
                .setSamplesCount(_samplesCount)
                .setStartTime(start)
                .setEndTime(stop)
                .setFileName(path)
                .setClusterName("test_cluster")
                .setServiceName("test_service")
                .build();
        generator.generate();

        benchmark(new File(Resources.getResource("application_perf_pipeline.json").toURI()), Duration.standardMinutes(90));
    }

    private final int _uowCount;
    private final int _namesCount;
    private final int _samplesCount;

    @Rule
    public final TestRule _benchmarkRule = new BenchmarkRule(JSON_BENCHMARK_CONSUMER);

    private static final JsonBenchmarkConsumer JSON_BENCHMARK_CONSUMER = new JsonBenchmarkConsumer(
            Paths.get("target/site/perf/benchmark-tsdagg.json"));

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationPipelinePT.class);
}
