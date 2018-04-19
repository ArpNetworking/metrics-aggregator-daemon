/**
 * Copyright 2018 Inscope Metrics, Inc
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
package com.arpnetworking.utility;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * Benchmark tests comparing the RegexAndMapReplacer class against the built-in regex replacement.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class RegexAndMapBenchmarkTest {

    @BenchmarkOptions(benchmarkRounds = 2000000, warmupRounds = 50000)
    @Test
    public void testRegexAndMap() {
        final String result = RegexAndMapReplacer.replaceAll(PATTERN, INPUT, REPLACE, new LinkedHashMap<>()).getReplacement();
    }

    @BenchmarkOptions(benchmarkRounds = 2000000, warmupRounds = 50000)
    @Test
    public void testRegex() {
        final String result = PATTERN.matcher(INPUT).replaceAll(REPLACE);
    }

    @Rule
    public TestRule _benchmarkRun = new BenchmarkRule();
    private static final String REPLACE = "this is a ${g1} pattern called ${g2}";
    private static final Pattern PATTERN = Pattern.compile("(?<g1>test)/pattern/(?<g2>foo)");
    private static final String INPUT = "test/pattern/foo";
}
