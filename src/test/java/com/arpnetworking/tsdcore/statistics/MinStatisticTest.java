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
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests the MinStatistic class.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class MinStatisticTest {

    @Test
    public void testName() {
        Assert.assertEquals("min", MIN_STATISTIC.getName());
    }

    @Test
    public void testStat() {
        final Statistic tp0 = MIN_STATISTIC;
        final List<Quantity> vals = TestBeanFactory.createSamples(ONE_TO_FIVE);
        final Quantity calculated = tp0.calculate(vals);
        Assert.assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setValue(1.0)
                                .setUnit(Unit.MILLISECOND)
                                .build()));
    }

    @Test
    public void testEquality() {
        Assert.assertFalse(MIN_STATISTIC.equals(null));
        Assert.assertFalse(MIN_STATISTIC.equals("ABC"));
        Assert.assertTrue(MIN_STATISTIC.equals(MIN_STATISTIC));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(MIN_STATISTIC.hashCode(), MIN_STATISTIC.hashCode());
    }

    @Test
    public void testAccumulator() {
        final Accumulator<Void> accumulator = (Accumulator<Void>) MIN_STATISTIC.createCalculator();
        accumulator.accumulate(new Quantity.Builder().setValue(12d).build());
        accumulator.accumulate(new Quantity.Builder().setValue(18d).build());
        accumulator.accumulate(new Quantity.Builder().setValue(5d).build());
        final CalculatedValue<Void> calculated = accumulator.calculate(Collections.emptyMap());
        Assert.assertEquals(calculated.getValue(), new Quantity.Builder().setValue(5.0).build());
    }

    private static final List<Double> ONE_TO_FIVE = Lists.newArrayList(1d, 2d, 3d, 4d, 5d);
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final MinStatistic MIN_STATISTIC = (MinStatistic) STATISTIC_FACTORY.getStatistic("min");
}
