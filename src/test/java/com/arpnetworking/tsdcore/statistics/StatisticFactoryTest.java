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
package com.arpnetworking.tsdcore.statistics;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Tests for {@link StatisticFactory}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@RunWith(Parameterized.class)
public class StatisticFactoryTest {

    public StatisticFactoryTest(final List<String> names, final Class<Statistic> clazz) {
        _names = names;
        _clazz = clazz;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> createParameters() {
        return Arrays.asList(
                a(l("mean"), MeanStatistic.class),
                a(l("sum"), SumStatistic.class),
                a(l("count", "n"), CountStatistic.class),
                a(l("p0", "tp0", "min"), MinStatistic.class),
                a(l("p50", "tp50", "median"), MedianStatistic.class),
                a(l("p75", "tp75"), TP75Statistic.class),
                a(l("p90", "tp90"), TP90Statistic.class),
                a(l("p95", "tp95"), TP95Statistic.class),
                a(l("p99", "tp99"), TP99Statistic.class),
                a(l("p99.9", "tp99.9", "p99p9", "tp99p9"), TP99p9Statistic.class),
                a(l("p100", "tp100", "max"), MaxStatistic.class)
        );
    }

    @Test
    public void testTryGetStatistic() {
        final StatisticFactory factory = new StatisticFactory();

        for (final String name : _names) {
            final Optional<Statistic> statistic = factory.tryGetStatistic(name);
            Assert.assertTrue("Not found: " + name, statistic.isPresent());
            Assert.assertTrue(
                    "Expected: " + _clazz + " but was: " + statistic.get().getClass(),
                    _clazz.isInstance(statistic.get()));
        }
    }

    @Test
    public void testNoStatistic() {
        final StatisticFactory factory = new StatisticFactory();
        final Optional<Statistic> statistic = factory.tryGetStatistic("notARealStatistic");
        Assert.assertFalse(statistic.isPresent());
    }


    private static Object[] a(final Object... objects) {
        return objects;
    }

    private static List<String> l(final String... objects) {
        return Lists.newArrayList(objects);
    }

    private final List<String> _names;
    private final Class<Statistic> _clazz;
}
