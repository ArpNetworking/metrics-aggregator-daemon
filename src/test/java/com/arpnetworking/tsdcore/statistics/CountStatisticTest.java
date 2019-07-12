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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests for the CountStatistic class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
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
        Assert.assertEquals("n", Iterables.getOnlyElement(statistic.getAliases()));
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
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(12d).build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(18d).build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue(5d).build());
        final CalculatedValue<?> calculated = accumulator.calculate(Collections.emptyMap());
        Assert.assertEquals(calculated.getValue(), new DefaultQuantity.Builder().setValue(3.0).build());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final CountStatistic COUNT_STATISTIC = (CountStatistic) STATISTIC_FACTORY.getStatistic("count");
}
