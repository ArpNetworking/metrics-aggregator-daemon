/**
 * Copyright 2014 Brandon Arp
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the <code>MultiSink</code> class.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class MultiSinkTest {

    @Before
    public void before() {
        _multiSinkBuilder = new MultiSink.Builder()
                .setName("multi_sink_test");
    }

    @Test
    public void testClose() {
        final Sink mockSinkA = Mockito.mock(Sink.class, "mockSinkA");
        final Sink mockSinkB = Mockito.mock(Sink.class, "mockSinkB");
        final Sink multiSink = _multiSinkBuilder
                .setSinks(Lists.newArrayList(mockSinkA, mockSinkB))
                .build();
        multiSink.close();
        Mockito.verify(mockSinkA).close();
        Mockito.verify(mockSinkB).close();
    }

    @Test
    public void testRecordProcessedAggregateData() {
        final ImmutableList<AggregatedData> data = ImmutableList.of(TestBeanFactory.createAggregatedData());
        final Sink mockSinkA = Mockito.mock(Sink.class, "mockSinkA");
        final Sink mockSinkB = Mockito.mock(Sink.class, "mockSinkB");
        final Sink multiSink = _multiSinkBuilder
                .setSinks(Lists.newArrayList(mockSinkA, mockSinkB))
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .build();
        multiSink.recordAggregateData(periodicData);
        Mockito.verify(mockSinkA).recordAggregateData(periodicData);
        Mockito.verify(mockSinkB).recordAggregateData(periodicData);
    }

    private MultiSink.Builder _multiSinkBuilder;
}
