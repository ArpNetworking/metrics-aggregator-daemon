/**
 * Copyright 2018 InscopeMetrics.com
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
import com.arpnetworking.metrics.mad.sources.TransformingSource.MergingMetric;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.test.UnorderedRecordEquality;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Map;

/**
 * Tests for the <code>MergingSource</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class TransformingSourceTest {

    @Before
    public void setUp() {
        _mockObserver = Mockito.mock(Observer.class);
        _mockSource = Mockito.mock(Source.class);
        _transformingSourceBuilder = new TransformingSource.Builder()
                .setName("TransformingSourceTest")
                .setTransformations(ImmutableList.of(
                        new TransformingSource.TransformationSet.Builder()
                                .setFindAndReplace(ImmutableMap.of(
                                        "foo/([^/]*)/bar", ImmutableList.of("foo/bar"),
                                        "cat/([^/]*)/dog", ImmutableList.of("cat/dog", "cat/dog/$1"),
                                        "tagged/([^/]*)/dog", ImmutableList.of("tagged/dog;animal=$1"),
                                        "named/(?<animal>[^/]*)", ImmutableList.of("named/extracted_animal;extracted=${animal}"),
                                        "tagged/([^/]*)/animal", ImmutableList.of("tagged/${animal}/animal")))
                        .build()
                ))
                .setSource(_mockSource);
    }

    @Test
    public void testAttach() {
        _transformingSourceBuilder.build();
        Mockito.verify(_mockSource).attach(Mockito.any(Observer.class));
    }

    @Test
    public void testStart() {
        _transformingSourceBuilder.build().start();
        Mockito.verify(_mockSource).start();
    }

    @Test
    public void testStop() {
        _transformingSourceBuilder.build().stop();
        Mockito.verify(_mockSource).stop();
    }

    @Test
    public void testToString() {
        final String asString = _transformingSourceBuilder.build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testMergingObserverInvalidEvent() {
        final TransformingSource transformingSource = new TransformingSource.Builder()
                .setName("testMergingObserverInvalidEventTransformingSource")
                .setSource(_mockSource)
                .setFindAndReplace(ImmutableMap.of())
                .build();
        Mockito.reset(_mockSource);
        new TransformingSource.TransformingObserver(
                transformingSource,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of())
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

        final Record actualRecord = mapRecord(nonMatchingRecord);

        assertRecordsEqual(actualRecord, nonMatchingRecord);
    }

    @Test
    public void testMergeTwoGauges() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "foo/1/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build(),
                        "foo/2/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

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
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testDropMetricOfDifferentType() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "foo/1/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder().setValue(1.23d).build()))
                                .build(),
                        "foo/2/bar",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.TIMER)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder().setValue(2.46d).build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

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
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

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
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testInjectsDimension() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setMetrics(matchingRecord.getMetrics())
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testExtractTagWithCapture() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "tagged/sheep/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.<String, String>builder().putAll(matchingRecord.getDimensions()).put("animal", "sheep").build())
                .setMetrics(ImmutableMap.of(
                        "tagged/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testInjectTagWithCapture() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "tagged/foo/animal",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "animal", "frog"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);
        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.remove("animal");

        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "tagged/frog/animal",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testMatchOverridesTagInCapture() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "named/frog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "animal", "cat"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.put("extracted", "frog");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "named/extracted_animal",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testStaticDimensionInjection() {
        _transformingSourceBuilder.setInject(ImmutableMap.of(
                "injected",
                new TransformingSource.DimensionInjection.Builder()
                        .setValue("value")
                        .setReplaceExisting(false)
                        .build()));
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.put("injected", "value");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testStaticDimensionInjectionOverwrite() {
        _transformingSourceBuilder.setInject(ImmutableMap.of(
                "injected",
                new TransformingSource.DimensionInjection.Builder()
                        .setValue("new_value")
                        .setReplaceExisting(true)
                        .build(),
                "injected_no_over",
                new TransformingSource.DimensionInjection.Builder()
                        .setValue("new_value")
                        .setReplaceExisting(false)
                        .build()));
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "injected", "old_value",
                                "injected_no_over", "old_value"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.put("injected", "new_value");
        expectedDimensions.put("injected_no_over", "old_value");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testRemoveDimension() {
        _transformingSourceBuilder.setRemove(ImmutableList.of("remove"));
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "remove", "_value"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.remove("remove");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testExtractOverridesExisting() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "named/frog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "extracted", "none"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.put("extracted", "frog");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "named/extracted_animal",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testReplaceWithCaptureWithTags() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "cat/sheep/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

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
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testMultipleMatches() {
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "cat/bear/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build(),
                        "cat/sheep/dog",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new Quantity.Builder()
                                                .setValue(2.46d)
                                                .build()))
                                .build()))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

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
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    private void assertRecordsEqual(final Record actualRecord, final Record expectedRecord) {
        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    private Record mapRecord(final Record record) {
        final Source transformingSource = _transformingSourceBuilder.build();
        transformingSource.attach(_mockObserver);
        notify(_mockSource, record);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(transformingSource), argument.capture());
        return argument.getValue();
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
    private TransformingSource.Builder _transformingSourceBuilder;

    private static final Observable OBSERVABLE = new Observable() {
        @Override
        public void attach(final Observer observer) {
        }

        @Override
        public void detach(final Observer observer) {
        }
    };
}
