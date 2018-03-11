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
import com.arpnetworking.tsdcore.model.PeriodicData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Collections;

/**
 * Tests for the <code>PeriodFilteringSink</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class PeriodFilteringSinkTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDefaultInclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testDefaultInclude")
                .setSink(_sink)
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1))
                .build();

        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void testExclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExclude")
                .setSink(_sink)
                .setExclude(Collections.singleton(Duration.ofMinutes(5)))
                .build();
        final PeriodicData periodicDataIn = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataIn);
        Mockito.verifyZeroInteractions(_sink);
    }

    @Test
    public void testExcludeLessThanExclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeLessThanExclude")
                .setSink(_sink)
                .setExcludeLessThan(Duration.ofMinutes(5))
                .build();
        final PeriodicData periodicDataExcluded = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataExcluded);
        Mockito.verifyZeroInteractions(_sink);
    }

    @Test
    public void testExcludeLessThanInclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeLessThanInclude")
                .setSink(_sink)
                .setExcludeLessThan(Duration.ofMinutes(5))
                .build();
        final PeriodicData periodicDataIncluded = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataIncluded);
        Mockito.verify(_sink).recordAggregateData(periodicDataIncluded);
    }

    @Test
    public void testExcludeGreaterThanExclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeGreaterThanExclude")
                .setSink(_sink)
                .setExcludeGreaterThan(Duration.ofMinutes(5))
                .build();
        final PeriodicData periodicDataExcluded = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(10))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataExcluded);
        Mockito.verifyZeroInteractions(_sink);
    }

    @Test
    public void testExcludeGreaterThanInclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeGreaterThanInclude")
                .setSink(_sink)
                .setExcludeGreaterThan(Duration.ofMinutes(5))
                .build();
        final PeriodicData periodicDataIncluded = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataIncluded);
        Mockito.verify(_sink).recordAggregateData(periodicDataIncluded);
    }

    @Test
    public void testIncludeOverExclude() {
        final Duration includePeriod = Duration.ofMinutes(5);
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testIncludeOverExclude")
                .setSink(_sink)
                .setInclude(Collections.singleton(includePeriod))
                .setExclude(Collections.singleton(includePeriod))
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void testIncludeOverLessThanExclude() {
        final Duration includePeriod = Duration.ofMinutes(5);
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testIncludeOverLessThanExclude")
                .setSink(_sink)
                .setInclude(Collections.singleton(includePeriod))
                .setExcludeLessThan(Duration.ofMinutes(10))
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(10))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void testIncludeOverGreaterThanExclude() {
        final Duration includePeriod = Duration.ofMinutes(5);
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testIncludeOverGreaterThanExclude")
                .setSink(_sink)
                .setInclude(Collections.singleton(includePeriod))
                .setExcludeGreaterThan(Duration.ofMinutes(10))
                .build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(10))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Mock
    private Sink _sink;
}
