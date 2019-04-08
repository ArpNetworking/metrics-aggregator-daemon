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
package io.inscopemetrics.mad.performance;

import com.arpnetworking.metrics.generator.util.TestFileGenerator;
import com.arpnetworking.test.junitbenchmarks.JsonBenchmarkConsumer;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;

/**
 * Perf tests that cover reading from a file and computing the aggregates
 * from it.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
@BenchmarkOptions(callgc = true, benchmarkRounds = 1, warmupRounds = 0)
public class ApplicationPipelineTestPerf extends FilePerfTestBase {

    public ApplicationPipelineTestPerf(
            final String name,
            final int uowCount,
            final int namesCount,
            final int samplesCount) {
        final Path path = Paths.get("target/tmp/perf/application-generated-sample-" + name + ".log");
        final Path parent = path.getParent();
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        final ZonedDateTime start = ZonedDateTime.now()
                .minusDays(1)
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0);
        final ZonedDateTime stop = start.plusMinutes(10);
        final TestFileGenerator generator = new TestFileGenerator.Builder()
                .setRandom(RANDOM)
                .setUnitOfWorkCount(uowCount)
                .setNamesCount(namesCount)
                .setSamplesCount(samplesCount)
                .setStartTime(start)
                .setEndTime(stop)
                .setFileName(path)
                .setClusterName("test_cluster")
                .setServiceName("test_service")
                .build();
        generator.generate();

        _file = path;
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

        final List<Object[]> params = Lists.newArrayList();
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
    public void test() throws IOException {
        LOGGER.info(String.format(
                "ApplicationPipeline Performance Test; uowCount=%d, namesCount=%d, samplesCount=%d",
                _uowCount, _namesCount, _samplesCount));

        benchmark(
                "application_perf_pipeline.json",
                Duration.ofMinutes(90),
                ImmutableMap.of(
                        "${SAMPLE_FILE}",
                        _file.toString()));
    }

    private final Path _file;
    private final int _uowCount;
    private final int _namesCount;
    private final int _samplesCount;

    @Rule
    public final TestRule _benchmarkRule = new BenchmarkRule(JSON_BENCHMARK_CONSUMER);

    private static final JsonBenchmarkConsumer JSON_BENCHMARK_CONSUMER = new JsonBenchmarkConsumer(
            Paths.get("target/site/perf/benchmark-application-mad.json"));

    private static final RandomGenerator RANDOM = new MersenneTwister(1298);
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationPipelineTestPerf.class);
}
