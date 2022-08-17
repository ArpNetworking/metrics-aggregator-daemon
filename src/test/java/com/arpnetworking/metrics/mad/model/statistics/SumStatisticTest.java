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
package com.arpnetworking.metrics.mad.model.statistics;

import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests for the SumStatistic class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class SumStatisticTest {

    @Test
    public void testGetName() {
        final Statistic stat = SUM_STATISTIC;
        MatcherAssert.assertThat(stat.getName(), Matchers.equalTo("sum"));
    }

    @Test
    public void testEquality() {
        Assert.assertFalse(SUM_STATISTIC.equals(null));
        Assert.assertFalse(SUM_STATISTIC.equals("ABC"));
        Assert.assertTrue(SUM_STATISTIC.equals(SUM_STATISTIC));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(SUM_STATISTIC.hashCode(), SUM_STATISTIC.hashCode());
    }

    @Test
    public void testAccumulatorWithSamples() {
        final Accumulator<Void> accumulator = (Accumulator<Void>) SUM_STATISTIC.createCalculator();
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(12d).build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(18d).build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(5d).build());
        final CalculatedValue<?> calculated = accumulator.calculate(Collections.emptyMap());
        Assert.assertEquals(calculated.getValue(), new DefaultQuantity.Builder().setValue(35.0).build());
    }

    @Test
    public void testAccumulatorWithCalculatedValues() {
        final Accumulator<Void> accumulator = (Accumulator<Void>) SUM_STATISTIC.createCalculator();
        accumulator.accumulate(
                new CalculatedValue.Builder<Void>()
                        .setValue(new DefaultQuantity.Builder().setValue(12d).build())
                        .build());
        accumulator.accumulate(
                new CalculatedValue.Builder<Void>()
                        .setValue(new DefaultQuantity.Builder().setValue(18d).build())
                        .build());
        accumulator.accumulate(
                new CalculatedValue.Builder<Void>()
                        .setValue(new DefaultQuantity.Builder().setValue(5d).build())
                        .build());
        final CalculatedValue<?> calculated = accumulator.calculate(Collections.emptyMap());
        Assert.assertEquals(calculated.getValue(), new DefaultQuantity.Builder().setValue(35.0).build());
    }

    @Test
    public void testAccumulatorMixed() {
        final Accumulator<Void> accumulator = (Accumulator<Void>) SUM_STATISTIC.createCalculator();
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(12d).build());
        accumulator.accumulate(
                new CalculatedValue.Builder<Void>()
                        .setValue(new DefaultQuantity.Builder().setValue(18d).build())
                        .build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(5d).build());
        final CalculatedValue<?> calculated = accumulator.calculate(Collections.emptyMap());
        Assert.assertEquals(calculated.getValue(), new DefaultQuantity.Builder().setValue(35.0).build());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final SumStatistic SUM_STATISTIC = (SumStatistic) STATISTIC_FACTORY.getStatistic("sum");
}
