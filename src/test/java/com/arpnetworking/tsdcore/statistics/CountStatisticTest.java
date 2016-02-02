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

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests for the CountStatistic class.
 *
 * @author Brandon Arp (barp at groupon dot com)
 */
public class CountStatisticTest {

    @Test
    public void testGetName() {
        final Statistic stat = COUNT_STATISTIC;
        Assert.assertThat(stat.getName(), Matchers.equalTo("count"));
    }

    @Test
    public void testAliases() {
        final Statistic statistic = COUNT_STATISTIC;
        Assert.assertEquals(1, statistic.getAliases().size());
        Assert.assertEquals("n", Iterables.getFirst(statistic.getAliases(), null));
    }

    @Test
    public void testCalculate() {
        final Statistic stat = COUNT_STATISTIC;
        final List<Double> doubleVals = Lists.newArrayList(12d, 18d, 5d);
        final List<Quantity> vals = TestBeanFactory.createSamples(doubleVals);
        final Quantity calculated = stat.calculate(vals);
        Assert.assertThat(calculated, Matchers.equalTo(new Quantity.Builder().setValue(3.0).build()));
    }

    @Test
    public void testEquality() {
        Assert.assertFalse(COUNT_STATISTIC.equals(null));
        Assert.assertFalse(COUNT_STATISTIC.equals("ABC"));
        Assert.assertTrue(COUNT_STATISTIC.equals(COUNT_STATISTIC));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(COUNT_STATISTIC.hashCode(), COUNT_STATISTIC.hashCode());
    }

    @Test
    public void testAccumulator() {
        final Accumulator<Void> accumulator = (Accumulator<Void>) COUNT_STATISTIC.createCalculator();
        accumulator.accumulate(new Quantity.Builder().setValue(12d).build());
        accumulator.accumulate(new Quantity.Builder().setValue(18d).build());
        accumulator.accumulate(new Quantity.Builder().setValue(5d).build());
        final CalculatedValue<?> calculated = accumulator.calculate(Collections.emptyMap());
        Assert.assertEquals(calculated.getValue(), new Quantity.Builder().setValue(3.0).build());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final CountStatistic COUNT_STATISTIC = (CountStatistic) STATISTIC_FACTORY.getStatistic("count");
}
