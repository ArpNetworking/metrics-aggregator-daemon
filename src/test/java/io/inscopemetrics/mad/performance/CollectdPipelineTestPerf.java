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

import com.arpnetworking.test.junitbenchmarks.JsonBenchmarkConsumer;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.zip.GZIPInputStream;

/**
 * Perf tests the collectd pipeline with a real collectd sample and a pipeline pulled from production.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@BenchmarkOptions(callgc = true, benchmarkRounds = 1, warmupRounds = 0)
public class CollectdPipelineTestPerf extends FilePerfTestBase {

    @BeforeClass
    public static void setUp() throws IOException, URISyntaxException {
        // Extract the sample file
        final Path gzipPath = Paths.get(Resources.getResource("collectd-sample1.log.gz").toURI());
        final FileInputStream fileInputStream = new FileInputStream(gzipPath.toFile());
        final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        final Path path = Paths.get("target/tmp/perf/collectd-sample1.log");
        final FileOutputStream outputStream = new FileOutputStream(path.toFile());

        ByteStreams.copy(gzipInputStream, outputStream);

        JSON_BENCHMARK_CONSUMER.prepareClass();
    }

    @Test
    public void test() throws IOException {
        benchmark(
                "collectd_sample1_pipeline.json",
                Duration.ofMinutes(20),
                ImmutableMap.of(
                        "${SAMPLE_FILE}",
                        "target/tmp/perf/collectd-sample1.log"));
    }

    @Rule
    public final TestRule _benchmarkRule = new BenchmarkRule(JSON_BENCHMARK_CONSUMER);

    private static final JsonBenchmarkConsumer JSON_BENCHMARK_CONSUMER = new JsonBenchmarkConsumer(
            Paths.get("target/site/perf/benchmark-collectd-mad.json"));
}
