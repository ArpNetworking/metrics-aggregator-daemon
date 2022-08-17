/*
 * Copyright 2015 Groupon.com
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

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Tests the HistogramStatistic class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class HistogramStatisticTest {

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic TP99_STATISTIC = STATISTIC_FACTORY.getStatistic("tp99");
    private static final HistogramStatistic HISTOGRAM_STATISTIC = (HistogramStatistic) STATISTIC_FACTORY.getStatistic("histogram");

    private final Supplier<HistogramStatistic.HistogramSupportingData.Builder> _histogramSupportingDataBuilder =
            () -> new HistogramStatistic.HistogramSupportingData.Builder()
                    .setHistogramSnapshot(new HistogramStatistic.HistogramAccumulator(TP99_STATISTIC)
                            .accumulate(TestBeanFactory.createSample())
                            .calculate(ImmutableMap.of())
                            .getData()
                            .getHistogramSnapshot())
                    .setUnit(Unit.SECOND);

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _histogramSupportingDataBuilder.get(),
                HistogramStatistic.HistogramSupportingData.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_histogramSupportingDataBuilder.get());
    }

    @Test
    public void testToString() {
        final String asString = _histogramSupportingDataBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void histogramAccumulateQuantities() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator.accumulate(new DefaultQuantity.Builder().setValue((double) x).build());
        }

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        for (final Map.Entry<Double, Long> entry : histogram.getValues()) {
            Assert.assertEquals(entry.getValue(), (Long) 1L);
        }
    }

    @Test
    public void histogramAccumulateHistogram() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> merged = HISTOGRAM_STATISTIC.createCalculator();

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator.accumulate(new DefaultQuantity.Builder().setValue((double) x).build());
        }

        merged.accumulate(accumulator.calculate(Collections.emptyMap()));

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = merged.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        for (final Map.Entry<Double, Long> entry : histogram.getValues()) {
            Assert.assertEquals(entry.getValue(), (Long) 1L);
        }
    }

    @Test
    public void histogramAccumulateMultipleHistogram() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> merged = HISTOGRAM_STATISTIC.createCalculator();

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator1 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator1.accumulate(new DefaultQuantity.Builder().setValue((double) x).build());
        }

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator2 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator2.accumulate(new DefaultQuantity.Builder().setValue((double) 10 * x + 1000).build());
        }

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator3 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 50; x <= 100; ++x) {
            accumulator3.accumulate(new DefaultQuantity.Builder().setValue((double) x).build());
        }

        merged.accumulate(accumulator1.calculate(Collections.emptyMap()));
        merged.accumulate(accumulator2.calculate(Collections.emptyMap()));
        merged.accumulate(accumulator3.calculate(Collections.emptyMap()));

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = merged.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        for (final Map.Entry<Double, Long> entry : histogram.getValues()) {
            final int val = entry.getKey().intValue();
            if (val < 50) {
                Assert.assertEquals("incorrect value for key " + val, (Long) 1L, entry.getValue());
            } else if (val <= 100) {
                Assert.assertEquals("incorrect value for key " + val, (Long) 2L, entry.getValue());
            } else { // val > 100
                Assert.assertEquals("incorrect value for key " + val, (Long) 1L, entry.getValue());
            }
        }

        Assert.assertEquals(2000d, histogram.getValueAtPercentile(99.9d), 1d);
    }

    @Test(expected = IllegalStateException.class)
    public void histogramQuantityInvalidConversion() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        accumulator.accumulate(new DefaultQuantity.Builder().setValue((double) 1).setUnit(null).build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue((double) 1000).setUnit(Unit.MILLISECOND).build());

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(2L, histogram.getEntriesCount());
        Assert.assertEquals(1, histogram.getValues().size());
    }

    @Test
    public void histogramEnds() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        accumulator.accumulate(new DefaultQuantity.Builder().setValue((double) 10).setUnit(Unit.SECOND).build());
        accumulator.accumulate(new DefaultQuantity.Builder().setValue((double) 50).setUnit(Unit.SECOND).build());

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();

        Assert.assertEquals(10d, histogram.getValueAtPercentile(0), 1d);
        Assert.assertEquals(50d, histogram.getValueAtPercentile(100), 1d);
    }

    @Test
    @SuppressFBWarnings(value = "FL_FLOATS_AS_LOOP_COUNTERS", justification = "Tests the floating point values")
    public void packAndUnpack() {
        final int precision = 7;
        final HistogramStatistic.Histogram histogram = new HistogramStatistic.Histogram(precision);

        // At precision 7 all values between -256 and 256 are fully represented
        for (double i = -256; i < 256; i += 1.0) {
            assertPacked(histogram, i);
        }

        // At precision 7 most of these values are truncated
        for (double i = -1000; i < 1000; i += 0.01) {
            assertPackedTruncated(histogram, i, 0.01);
        }
    }

    private void assertPacked(final HistogramStatistic.Histogram histogram, final double val) {
        final long packed = histogram.pack(val);
        final double unpacked = histogram.unpack(packed);
        Assert.assertEquals(
                String.format("val: %f, unpacked: %f", val, unpacked),
                Double.doubleToLongBits(val),
                Double.doubleToLongBits(unpacked));
    }

    private void assertPackedTruncated(final HistogramStatistic.Histogram histogram, final double val, final double error) {
        final long packed = histogram.pack(val);
        final double unpacked = histogram.unpack(packed);
        Assert.assertEquals(histogram.truncateToLong(val), Double.doubleToLongBits(unpacked));
        Assert.assertTrue(Math.abs(val - unpacked) / val < error);
    }
}
