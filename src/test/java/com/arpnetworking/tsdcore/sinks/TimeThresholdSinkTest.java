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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;

/**
 * Tests for the {@link TimeThresholdSink}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class TimeThresholdSinkTest {
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void doesNotDropFreshData() {
        final TimeThresholdSink periodFilteringSink = new TimeThresholdSink.Builder()
                .setName("testKeepFresh")
                .setThreshold(Duration.ofMinutes(10))
                .setSink(_sink)
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1))
                .setStart(ZonedDateTime.now())
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void dropsOldDataByDefault() {
        final TimeThresholdSink periodFilteringSink = new TimeThresholdSink.Builder()
                .setName("testDropOld")
                .setThreshold(Duration.ofMinutes(10))
                .setSink(_sink)
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1))
                .setStart(ZonedDateTime.now().minus(Duration.ofMinutes(30)))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink, Mockito.never()).recordAggregateData(Mockito.any(PeriodicData.class));
    }

    @Test
    public void doesNotDropOldDataWhenLogOnly() {
        final TimeThresholdSink periodFilteringSink = new TimeThresholdSink.Builder()
                .setName("testKeepLogOnly")
                .setThreshold(Duration.ofMinutes(10))
                .setLogOnly(true)
                .setSink(_sink)
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1))
                .setStart(ZonedDateTime.now().minus(Duration.ofMinutes(30)))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void doesNotDropDataForExcludedServices() {
        final TimeThresholdSink periodFilteringSink = new TimeThresholdSink.Builder()
                .setName("testKeepsExcludedServices")
                .setThreshold(Duration.ofMinutes(10))
                .setExcludedServices(Collections.singleton("excluded"))
                .setSink(_sink)
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setDimensions(new DefaultKey(ImmutableMap.of(
                        Key.HOST_DIMENSION_KEY, "MyHost",
                        Key.SERVICE_DIMENSION_KEY, "excluded",
                        Key.CLUSTER_DIMENSION_KEY, "MyCluster")))
                .setPeriod(Duration.ofMinutes(1))
                .setStart(ZonedDateTime.now().minus(Duration.ofMinutes(30)))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Mock
    private Sink _sink;
}
