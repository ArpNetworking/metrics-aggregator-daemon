/**
 * Copyright 2016 Inscope Metrics Inc.
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.arpnetworking.tsdcore.statistics.HistogramStatistic;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.util.Map;

/**
 * Implementation of <code>Sink</code> which maps values in one unit to another.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class UnitMappingSink extends BaseSink {

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        final ImmutableMultimap.Builder<String, AggregatedData> dataBuilder = ImmutableMultimap.builder();
        for (final Map.Entry<String, AggregatedData> entry : periodicData.getData().entries()) {
            final Object supportingData = mapSupportingData(entry.getValue().getSupportingData());
            final Quantity value = mapQuantity(entry.getValue().getValue());

            dataBuilder.put(
                    entry.getKey(),
                    AggregatedData.Builder.<AggregatedData, AggregatedData.Builder>clone(entry.getValue())
                            .setValue(value)
                            .setSupportingData(supportingData)
                            .build());
        }
        _sink.recordAggregateData(
                PeriodicData.Builder.clone(periodicData, new PeriodicData.Builder())
                        .setData(dataBuilder.build())
                        .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        _sink.close();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("sink", _sink)
                .put("map", _map)
                .build();
    }

    private Quantity mapQuantity(final Quantity quantity) {
        if (quantity.getUnit().isPresent()) {
            final Unit fromUnit = quantity.getUnit().get();
            final Unit toUnit = _map.get(fromUnit);
            if (toUnit != null) {
                return new Quantity.Builder()
                        .setValue(toUnit.convert(quantity.getValue(), fromUnit))
                        .setUnit(toUnit)
                        .build();
            }
        }
        return quantity;
    }

    private Object mapSupportingData(final Object supportingData) {
        if (supportingData instanceof HistogramStatistic.HistogramSupportingData) {
            final HistogramStatistic.HistogramSupportingData hsd = (HistogramStatistic.HistogramSupportingData) supportingData;
            if (hsd.getUnit().isPresent()) {
                final Unit toUnit = _map.get(hsd.getUnit().get());
                if (toUnit != null) {
                    return hsd.toUnit(toUnit);
                }
            }
        }
        return supportingData;
    }

    private UnitMappingSink(final Builder builder) {
        super(builder);
        _map = Maps.newHashMap(builder._map);
        _sink = builder._sink;
    }

    private final Map<Unit, Unit> _map;
    private final Sink _sink;

    /**
     * Implementation of builder pattern for <code>UnitMappingSink</code>.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, UnitMappingSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(UnitMappingSink::new);
        }

        /**
         * The map of unit to unit. Cannot be null.
         *
         * @param value The map of unit to unit.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setMap(final Map<Unit, Unit> value) {
            _map = value;
            return self();
        }

        /**
         * The sink to wrap. Cannot be null.
         *
         * @param value The sink to wrap.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Map<Unit, Unit> _map;
        @NotNull
        private Sink _sink;
    }
}
