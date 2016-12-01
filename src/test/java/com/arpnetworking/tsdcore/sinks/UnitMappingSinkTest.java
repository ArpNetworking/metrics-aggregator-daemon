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

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

/**
 * Tests for the <code>UnitMappingSink</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class UnitMappingSinkTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        _sink = new UnitMappingSink.Builder()
                .setMap(Collections.singletonMap(Unit.BIT, Unit.BYTE))
                .setSink(_target)
                .setName("UnitMappingSinkTest")
                .build();
    }

    @Test
    public void testMapping() {
        _sink.recordAggregateData(
                TestBeanFactory.createPeriodicDataBuilder()
                        .setData(
                                ImmutableMultimap.of(
                                        "my_metric",
                                        TestBeanFactory.createAggregatedDataBuilder()
                                                .setValue(
                                                        new Quantity.Builder()
                                                                .setValue(32d)
                                                                .setUnit(Unit.BIT)
                                                                .build())
                                                .build())
                        )
                        .build());

        Mockito.verify(_target).recordAggregateData(_periodicDataCaptor.capture());
        final PeriodicData periodicData = _periodicDataCaptor.getValue();
        final ImmutableCollection<AggregatedData> aggregates = periodicData.getData().get("my_metric");
        Assert.assertNotNull(aggregates);
        Assert.assertEquals(1, aggregates.size());
        Assert.assertEquals(Unit.BYTE, aggregates.iterator().next().getValue().getUnit().orElse(null));
        Assert.assertEquals(4d, aggregates.iterator().next().getValue().getValue(), 0.001);
    }

    @Test
    public void testNoMapping() {
        _sink.recordAggregateData(
                TestBeanFactory.createPeriodicDataBuilder()
                        .setData(
                                ImmutableMultimap.of(
                                        "my_metric",
                                        TestBeanFactory.createAggregatedDataBuilder()
                                                .setValue(
                                                        new Quantity.Builder()
                                                                .setValue(16d)
                                                                .setUnit(Unit.GIGABIT)
                                                                .build())
                                                .build())
                        )
                        .build());

        Mockito.verify(_target).recordAggregateData(_periodicDataCaptor.capture());
        final PeriodicData periodicData = _periodicDataCaptor.getValue();
        final ImmutableCollection<AggregatedData> aggregates = periodicData.getData().get("my_metric");
        Assert.assertNotNull(aggregates);
        Assert.assertEquals(1, aggregates.size());
        Assert.assertEquals(Unit.GIGABIT, aggregates.iterator().next().getValue().getUnit().orElse(null));
        Assert.assertEquals(16d, aggregates.iterator().next().getValue().getValue(), 0.001);
    }

    private Sink _sink;
    @Captor
    private ArgumentCaptor<PeriodicData> _periodicDataCaptor;
    @Mock
    private Sink _target;
}
