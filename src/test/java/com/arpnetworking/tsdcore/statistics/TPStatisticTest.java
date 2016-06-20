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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests the TPStatistic class.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class TPStatisticTest {

    @Test
    public void testName() {
        Assert.assertEquals("tp90", TP90_STATISTIC.getName());
        Assert.assertEquals("tp95", TP95_STATISTIC.getName());
        Assert.assertEquals("tp99", TP99_STATISTIC.getName());
        Assert.assertEquals("tp99.9", TP99P9_STATISTIC.getName());
    }

    @Test
    public void testTP99StatSmallSet() {
        final Statistic tp = TP99_STATISTIC;
        final List<Double> doubleVals = Lists.newArrayList(ONE_TO_FIVE);
        final List<Quantity> vals = TestBeanFactory.createSamples(doubleVals);
        final Quantity calculated = tp.calculate(vals);
        Assert.assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setValue(5.0)
                                .setUnit(Unit.MILLISECOND)
                                .build()));
    }

    @Test
    public void testTP99Stat() {
        final Statistic tp = TP99_STATISTIC;
        final ArrayList<Double> vList = Lists.newArrayList();
        for (int x = 0; x < 100; ++x) {
            vList.add((double) x);
        }
        final List<Quantity> vals = TestBeanFactory.createSamples(vList);
        final Quantity calculated = tp.calculate(vals);
        Assert.assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setUnit(Unit.MILLISECOND)
                                .setValue(99.0)
                                .build()));
    }

    @Test
    public void testTP999Stat() {
        final Statistic tp = TP99P9_STATISTIC;
        final ArrayList<Double> vList = Lists.newArrayList();
        for (int x = 0; x < 10000; ++x) {
            vList.add((double) x);
        }
        final List<Quantity> vals = TestBeanFactory.createSamples(vList);
        final Quantity calculated = tp.calculate(vals);
        Assert.assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setValue(9990.0)
                                .setUnit(Unit.MILLISECOND)
                                .build()));
    }

    @Test
    public void testEquality() {
        Assert.assertFalse(TP90_STATISTIC.equals(null));
        Assert.assertFalse(TP90_STATISTIC.equals("ABC"));
        Assert.assertTrue(TP90_STATISTIC.equals(TP90_STATISTIC));

        Assert.assertFalse(TP95_STATISTIC.equals(null));
        Assert.assertFalse(TP95_STATISTIC.equals("ABC"));
        Assert.assertTrue(TP95_STATISTIC.equals(TP95_STATISTIC));

        Assert.assertFalse(TP99_STATISTIC.equals(null));
        Assert.assertFalse(TP99_STATISTIC.equals("ABC"));
        Assert.assertTrue(TP99_STATISTIC.equals(TP99_STATISTIC));

        Assert.assertFalse(TP99P9_STATISTIC.equals(null));
        Assert.assertFalse(TP99P9_STATISTIC.equals("ABC"));
        Assert.assertTrue(TP99P9_STATISTIC.equals(TP99P9_STATISTIC));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(TP90_STATISTIC.hashCode(), TP90_STATISTIC.hashCode());
        Assert.assertEquals(TP95_STATISTIC.hashCode(), TP95_STATISTIC.hashCode());
        Assert.assertEquals(TP99_STATISTIC.hashCode(), TP99_STATISTIC.hashCode());
        Assert.assertEquals(TP99P9_STATISTIC.hashCode(), TP99P9_STATISTIC.hashCode());
    }

    @Test
    public void testTP90Accumulator() {
        final Accumulator<?> accumulator = (Accumulator<?>) HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 10000; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }
        final CalculatedValue<Void> calculated = TP90_STATISTIC.createCalculator().calculate(
                Collections.singletonMap(HISTOGRAM_STATISTIC, accumulator));
        Assert.assertTrue(areClose(new Quantity.Builder().setValue(9000.0).build(), calculated.getValue()));
    }

    @Test
    public void testTP95Accumulator() {
        final Accumulator<?> accumulator = (Accumulator<?>) HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 10000; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }
        final CalculatedValue<?> calculated = TP95_STATISTIC.createCalculator().calculate(
                Collections.singletonMap(HISTOGRAM_STATISTIC, accumulator));
        Assert.assertTrue(areClose(new Quantity.Builder().setValue(9500.0).build(), calculated.getValue()));
    }

    @Test
    public void testTP99Accumulator() {
        final Accumulator<?> accumulator = (Accumulator<?>) HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 10000; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }
        final CalculatedValue<?> calculated = TP99_STATISTIC.createCalculator().calculate(
                Collections.singletonMap(HISTOGRAM_STATISTIC, accumulator));
        Assert.assertTrue(areClose(new Quantity.Builder().setValue(9900.0).build(), calculated.getValue()));
    }

    @Test
    public void testTP99p9Accumulator() {
        final Accumulator<?> accumulator = (Accumulator<?>) HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 10000; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }
        final CalculatedValue<?> calculated = TP99P9_STATISTIC.createCalculator().calculate(
                Collections.singletonMap(HISTOGRAM_STATISTIC, accumulator));
        Assert.assertTrue(areClose(new Quantity.Builder().setValue(9990.0).build(), calculated.getValue()));
    }
    @Test
    public void testTP99p9AccumulatorBiModal() {
        final Accumulator<?> accumulator = (Accumulator<?>) HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 9900; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) 10).build());
        }
        for (int x = 1; x <= 100; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) 100).build());
        }
        final CalculatedValue<?> calculated = TP99P9_STATISTIC.createCalculator().calculate(
                Collections.singletonMap(HISTOGRAM_STATISTIC, accumulator));
        Assert.assertTrue(areClose(new Quantity.Builder().setValue(100.0).build(), calculated.getValue()));
    }

    private boolean areClose(final Quantity expected, final Quantity actual) {
        final double diff = Math.abs(expected.getValue() - actual.getValue());
        return Math.abs(diff / expected.getValue()) <= 0.01;
    }

    private static final List<Double> ONE_TO_FIVE = Lists.newArrayList(1d, 2d, 3d, 4d, 5d);
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic HISTOGRAM_STATISTIC = STATISTIC_FACTORY.getStatistic("histogram");
    private static final TP90Statistic TP90_STATISTIC = (TP90Statistic) STATISTIC_FACTORY.getStatistic("tp90");
    private static final TP95Statistic TP95_STATISTIC = (TP95Statistic) STATISTIC_FACTORY.getStatistic("tp95");
    private static final TP99Statistic TP99_STATISTIC = (TP99Statistic) STATISTIC_FACTORY.getStatistic("tp99");
    private static final TP99p9Statistic TP99P9_STATISTIC = (TP99p9Statistic) STATISTIC_FACTORY.getStatistic("tp99p9");
}
