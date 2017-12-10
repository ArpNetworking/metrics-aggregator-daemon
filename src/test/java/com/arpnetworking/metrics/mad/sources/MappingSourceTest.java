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
package com.arpnetworking.metrics.mad.sources;

import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.sources.MappingSource.MergingMetric;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.test.UnorderedRecordEquality;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Tests for the <code>MergingSource</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class MappingSourceTest {

    @Before
    public void setUp() {
        _mockObserver = Mockito.mock(Observer.class);
        _mockSource = Mockito.mock(Source.class);
        _mappingSourceBuilder = new MappingSource.Builder()
                .setName("MergingSourceTest")
                .setFindAndReplace(ImmutableMap.of(
                        "foo/([^/]*)/bar", ImmutableList.of("foo/bar"),
                        "cat/([^/]*)/dog", ImmutableList.of("cat/dog", "cat/dog/$1")))
                .setSource(_mockSource);
    }

    @Test
    public void testAttach() {
        _mappingSourceBuilder.build();
        Mockito.verify(_mockSource).attach(Mockito.any(Observer.class));
    }

    @Test
    public void testStart() {
        _mappingSourceBuilder.build().start();
        Mockito.verify(_mockSource).start();
    }

    @Test
    public void testStop() {
        _mappingSourceBuilder.build().stop();
        Mockito.verify(_mockSource).stop();
    }

    @Test
    public void testToString() {
        final String asString = _mappingSourceBuilder.build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testMergingObserverInvalidEvent() {
        final MappingSource mappingSource = new MappingSource.Builder()
                .setName("testMergingObserverInvalidEventMappingSource")
                .setSource(_mockSource)
                .setFindAndReplace(Collections.<String, List<String>>emptyMap())
                .build();
        Mockito.reset(_mockSource);
        new MappingSource.MappingObserver(
                mappingSource,
                Collections.<Pattern, List<String>>emptyMap())
                .notify(OBSERVABLE, "Not a Record");
        Mockito.verifyZeroInteractions(_mockSource);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergingMetricMergeMismatchedTypes() {
        final MergingMetric mergingMetric = new MergingMetric(
                TestBeanFactory.createMetricBuilder()
                        .setType(MetricType.COUNTER)
                        .build());
        mergingMetric.merge(TestBeanFactory.createMetricBuilder()
                .setType(MetricType.GAUGE)
                .build());
    }

    @Test
    public void testMergeNotMatch() {
        final Record nonMatchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "does_not_match",
                        TestBeanFactory.createMetric()))
                .build();

        final Source mergingSource = _mappingSourceBuilder.build();
        mergingSource.attach(_mockObserver);
        notify(_mockSource, nonMatchingRecord);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(mergingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        Assert.assertTrue(
                String.format("expected=%s, actual=%s", nonMatchingRecord, actualRecord),
                UnorderedRecordEquality.equals(nonMatchingRecord, actualRecord));
    }

    @Test
    public void testMergeTwoGauges() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "foo/1/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build(),
                        "foo/2/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build()))
                .build();

        final Source mergingSource = _mappingSourceBuilder.build();
        mergingSource.attach(_mockObserver);
        notify(_mockSource, matchingRecord);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(mergingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setMetrics(ImmutableMap.of(
                        "foo/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build(),
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build()))
                .build();
        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    @Test
    public void testDropMetricOfDifferentType() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "foo/1/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder().setValue(1.23d).build()))
                                .build(),
                        "foo/2/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.TIMER)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder().setValue(2.46d).build()))
                                .build()))
                .build();

        final Source mergingSource = _mappingSourceBuilder.build();
        mergingSource.attach(_mockObserver);
        notify(_mockSource, matchingRecord);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(mergingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        final Record expectedRecord1 = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setMetrics(ImmutableMap.of(
                        "foo/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder().setValue(1.23d).build()))
                                .build()))
                .build();
        final Record expectedRecord2 = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setMetrics(ImmutableMap.of(
                        "foo/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.TIMER)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder().setValue(2.46d).build()))
                                .build()))
                .build();

        Assert.assertTrue(
                String.format("expected1=%s OR expected2=%s, actual=%s", expectedRecord1, expectedRecord2, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord1, actualRecord)
                        || UnorderedRecordEquality.equals(expectedRecord2, actualRecord));
    }

    @Test
    public void testReplaceWithCapture() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "cat/sheep/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();

        final Source mergingSource = _mappingSourceBuilder.build();
        mergingSource.attach(_mockObserver);
        notify(_mockSource, matchingRecord);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(mergingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setMetrics(ImmutableMap.of(
                        "cat/dog/sheep",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    @Test
    public void testMultipleMatches() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "cat/bear/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build(),
                        "cat/sheep/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(Collections.singletonList(
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build()))
                .build();

        final Source mappingSource = _mappingSourceBuilder.build();
        mappingSource.attach(_mockObserver);
        notify(_mockSource, matchingRecord);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(mappingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setMetrics(ImmutableMap.of(
                        "cat/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build(),
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build(),
                        "cat/dog/sheep",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build(),
                        "cat/dog/bear",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    private static void notify(final Observable observable, final Object event) {
        final ArgumentCaptor<Observer> argument = ArgumentCaptor.forClass(Observer.class);
        Mockito.verify(observable).attach(argument.capture());
        for (final Observer observer : argument.getAllValues()) {
            observer.notify(observable, event);
        }
    }

    private Observer _mockObserver;
    private Source _mockSource;
    private MappingSource.Builder _mappingSourceBuilder;

    private static final Observable OBSERVABLE = new Observable() {
        @Override
        public void attach(final Observer observer) {
        }

        @Override
        public void detach(final Observer observer) {
        }
    };
}
