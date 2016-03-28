/**
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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.base.Optional;
import net.sf.oval.constraint.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Histogram statistic. This is a supporting statistic and does not produce
 * a value itself. It is used by percentile statistics as a common dependency.
 * Use <code>StatisticFactory</code> for construction.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public final class HistogramStatistic extends BaseStatistic {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "histogram";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Accumulator<HistogramSupportingData> createCalculator() {
        return new HistogramAccumulator(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculate(final List<Quantity> values) {
        throw new UnsupportedOperationException("Unsupported operation: calculate(List<Quantity>)");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        throw new UnsupportedOperationException("Unsupported operation: calculateAggregations(List<AggregatedData>)");
    }

    private HistogramStatistic() { }

    private static final long serialVersionUID = 7060886488604176233L;

    /**
     * Accumulator computing the histogram of values. There is a dependency on the
     * histogram accumulator from each percentile statistic's calculator.
     *
     * @author Ville Koskela (vkoskela at groupon dot com)
     */
    /* package private */ static final class HistogramAccumulator
            extends BaseCalculator<HistogramSupportingData>
            implements Accumulator<HistogramSupportingData> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>Statistic</code>.
         */
        /* package private */ HistogramAccumulator(final Statistic statistic) {
            super(statistic);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Accumulator<HistogramSupportingData> accumulate(final Quantity quantity) {
            // TODO(barp): Convert to canonical unit. [NEXT]
            final Optional<Unit> quantityUnit = quantity.getUnit();
            checkUnit(quantityUnit);
            if (_unit.isPresent() && !_unit.equals(quantityUnit)) {
                _histogram.recordValue(
                        new Quantity.Builder()
                                .setUnit(_unit.get())
                                .setValue(_unit.get().convert(quantity.getValue(), quantityUnit.get()))
                                .build().getValue());
            } else {
                _histogram.recordValue(quantity.getValue());
            }

            _unit = _unit.or(quantityUnit);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Accumulator<HistogramSupportingData> accumulate(final CalculatedValue<HistogramSupportingData> calculatedValue) {
            final Optional<Unit> unit = calculatedValue.getData().getUnit();
            checkUnit(unit);
            if (_unit.isPresent() && !_unit.equals(unit)) {
                _histogram.add(calculatedValue.getData().toUnit(_unit.get()).getHistogramSnapshot());
            } else {
                _histogram.add(calculatedValue.getData().getHistogramSnapshot());
            }

            _unit = _unit.or(unit);
            return this;
        }

        private void checkUnit(final Optional<Unit> unit) {
            if (_unit.isPresent() != unit.isPresent() && _histogram._entriesCount > 0) {
                throw new IllegalStateException(String.format(
                        "Units must both be present or absent; histogramUnit=%s, otherUnit=%s",
                        _unit,
                        unit));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CalculatedValue<HistogramSupportingData> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return new CalculatedValue.Builder<HistogramSupportingData>()
                    .setValue(new Quantity.Builder()
                            .setValue(1.0)
                            .build())
                    .setData(new HistogramSupportingData.Builder()
                            .setHistogramSnapshot(_histogram.getSnapshot())
                            .setUnit(_unit)
                            .build())
                    .build();
        }

        /**
         * Calculate the value at the specified percentile.
         *
         * @param percentile The desired percentile to calculate.
         * @return The value at the desired percentile.
         */
        public Quantity calculate(final double percentile) {
            final HistogramSnapshot snapshot = _histogram.getSnapshot();
            return new Quantity.Builder()
                    .setValue(snapshot.getValueAtPercentile(percentile))
                    .setUnit(_unit.orNull())
                    .build();
        }

        private Optional<Unit> _unit = Optional.absent();
        private final Histogram _histogram = new Histogram();
    }

    /**
     * Supporting data based on a histogram.
     *
     * @author Brandon Arp (barp at groupon dot com)
     */
    public static final class HistogramSupportingData {
        /**
         * Public constructor.
         *
         * @param builder The builder.
         */
        public HistogramSupportingData(final Builder builder) {
            _unit = builder._unit;
            _histogramSnapshot = builder._histogramSnapshot;
        }

        public HistogramSnapshot getHistogramSnapshot() {
            return _histogramSnapshot;
        }

        /**
         * Transforms the histogram to a new unit. If there is no unit set,
         * the result is a no-op.
         *
         * @param newUnit the new unit
         * @return a new HistogramSupportingData with the units converted
         */
        public HistogramSupportingData toUnit(final Unit newUnit) {
            if (_unit.isPresent()) {
                final Histogram newHistogram = new Histogram();
                for (final Map.Entry<Double, Integer> entry : _histogramSnapshot.getValues()) {
                    final Double newBucket = newUnit.convert(entry.getKey(), _unit.get());
                    newHistogram.recordValue(newBucket, entry.getValue());
                }
                return new HistogramSupportingData.Builder()
                        .setHistogramSnapshot(newHistogram.getSnapshot())
                        .setUnit(Optional.of(newUnit))
                        .build();
            }
            return this;
        }

        public Optional<Unit> getUnit() {
            return _unit;
        }

        private final Optional<Unit> _unit;
        private final HistogramSnapshot _histogramSnapshot;

        /**
         * Implementation of the builder pattern for a {@link HistogramSupportingData}.
         *
         * @author Brandon Arp (barp at groupon dot com)
         */
        public static class Builder extends OvalBuilder<HistogramSupportingData> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(HistogramSupportingData::new);
            }

            /**
             * Sets the histogram. Required. Cannot be null.
             *
             * @param value the histogram
             * @return This {@link Builder} instance.
             */
            public Builder setHistogramSnapshot(final HistogramSnapshot value) {
                _histogramSnapshot = value;
                return this;
            }

            /**
             * Sets the unit. Optional. Cannot be null.
             *
             * @param value the unit
             * @return This {@link Builder} instance.
             */
            public Builder setUnit(final Optional<Unit> value) {
                _unit = value;
                return this;
            }

            @NotNull
            private Optional<Unit> _unit = Optional.absent();
            @NotNull
            private HistogramSnapshot _histogramSnapshot;
        }
    }

    /**
     * A simple histogram implementation.
     */
    public static final class Histogram {

        /**
         * Records a value into the histogram.
         *
         * @param value The value of the entry.
         * @param count The number of entries at this value.
         */
        public void recordValue(final double value, final int count) {
            _data.merge(truncate(value), count, (i, j) -> i + j);
            _entriesCount += count;
        }

        /**
         * Records a value into the histogram.
         *
         * @param value The value of the entry.
         */
        public void recordValue(final double value) {
            recordValue(value, 1);
        }

        /**
         * Adds a histogram snapshot to this one.
         *
         * @param histogramSnapshot The histogram snapshot to add to this one.
         */
        public void add(final HistogramSnapshot histogramSnapshot) {
            for (final Map.Entry<Double, Integer> entry : histogramSnapshot._data.entrySet()) {
                _data.merge(entry.getKey(), entry.getValue(), (i, j) -> i + j);
            }
            _entriesCount += histogramSnapshot._entriesCount;
        }

        public HistogramSnapshot getSnapshot() {
            return new HistogramSnapshot(_data, _entriesCount);
        }

        private static double truncate(final double val) {
            final long mask = 0xffffe00000000000L;
            return Double.longBitsToDouble(Double.doubleToRawLongBits(val) & mask);
        }

        private int _entriesCount = 0;
        private final TreeMap<Double, Integer> _data = new TreeMap<>();
    }

    /**
     * Represents a snapshot of immutable histogram data.
     *
     * @author Brandon Arp (barp at groupon dot com)
     */
    public static final class HistogramSnapshot {
        private HistogramSnapshot(final TreeMap<Double, Integer> data, final int entriesCount) {
            _entriesCount = entriesCount;
            _data.putAll(data);
        }

        /**
         * Gets the value of the bucket that corresponds to the percentile.
         *
         * @param percentile the percentile
         * @return The value of the bucket at the percentile.
         */
        public Double getValueAtPercentile(final double percentile) {
            // Always "round up" on fractional samples to bias toward 100%
            // The Math.min is for the case where the computation may be just
            // slightly larger than the _entriesCount and prevents an index out of range.
            final int target = (int) Math.min(Math.ceil(_entriesCount * percentile / 100.0D), _entriesCount);
            int accumulated = 0;
            for (final Map.Entry<Double, Integer> next : _data.entrySet()) {
                accumulated += next.getValue();
                if (accumulated >= target) {
                    return next.getKey();
                }
            }
            return 0D;
        }

        public int getEntriesCount() {
            return _entriesCount;
        }

        public Set<Map.Entry<Double, Integer>> getValues() {
            return _data.entrySet();
        }
        private int _entriesCount = 0;
        private final TreeMap<Double, Integer> _data = new TreeMap<>();
    }
}
