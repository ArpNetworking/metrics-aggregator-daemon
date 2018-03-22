/**
 * Copyright 2018 Inscope Metrics, Inc
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
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.test.UnorderedRecordEquality;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;

/**
 * Tests for the {@link DimensionInjectingSource} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class DimensionInjectingSourceTest {


    @Before
    public void setUp() {
        _mockObserver = Mockito.mock(Observer.class);
        _mockSource = Mockito.mock(Source.class);
        _dimensions = ImmutableMap.of(
                        "foo", "bar",
                        "a", "b");
        _sourceBuilder = new DimensionInjectingSource.Builder()
                .setName("MergingSourceTest")
                .setDimensions(_dimensions)
                .setSource(_mockSource);
    }

    @Test
    public void testAttach() {
        _sourceBuilder.build();
        Mockito.verify(_mockSource).attach(Mockito.any(Observer.class));
    }

    @Test
    public void testStart() {
        _sourceBuilder.build().start();
        Mockito.verify(_mockSource).start();
    }

    @Test
    public void testStop() {
        _sourceBuilder.build().stop();
        Mockito.verify(_mockSource).stop();
    }

    @Test
    public void testToString() {
        final String asString = _sourceBuilder.build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testObserverInvalidEvent() {
        final DimensionInjectingSource mappingSource = new DimensionInjectingSource.Builder()
                .setName("testMergingObserverInvalidEventMappingSource")
                .setSource(_mockSource)
                .setDimensions(Collections.emptyMap())
                .build();
        Mockito.reset(_mockSource);
        new DimensionInjectingSource.InjectingObserver(
                mappingSource,
                false,
                ImmutableMap.of())
                .notify(OBSERVABLE, "Not a Record");
        Mockito.verifyZeroInteractions(_mockSource);
    }

    @Test
    public void testInjectsDimensions() {
        final Record record = TestBeanFactory.createRecordBuilder().build();

        final HashMap<String, String> effectiveDimensions = Maps.newHashMap(record.getDimensions());
        effectiveDimensions.putAll(_dimensions);
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(record.getAnnotations())
                .setTime(record.getTime())
                .setDimensions(ImmutableMap.copyOf(effectiveDimensions))
                .build();

        final Source injectingSource = _sourceBuilder.build();
        injectingSource.attach(_mockObserver);
        notify(_mockSource, record);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(injectingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    @Test
    public void testInjectDimensionsReplace() {
        final Record record = TestBeanFactory.createRecordBuilder()
                .setDimensions(ImmutableMap.of("a", "notb", "some", "thing"))
                .build();

        final HashMap<String, String> effectiveDimensions = Maps.newHashMap(record.getDimensions());
        effectiveDimensions.putAll(_dimensions);
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(record.getAnnotations())
                .setTime(record.getTime())
                .setDimensions(ImmutableMap.copyOf(effectiveDimensions))
                .build();

        final Source injectingSource = _sourceBuilder.build();
        injectingSource.attach(_mockObserver);
        notify(_mockSource, record);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(injectingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    @Test
    public void testInjectDimensionsNoReplace() {
        final Record record = TestBeanFactory.createRecordBuilder()
                .setDimensions(ImmutableMap.of("a", "notb", "some", "thing"))
                .build();

        final HashMap<String, String> effectiveDimensions = Maps.newHashMap(_dimensions);
        effectiveDimensions.putAll(record.getDimensions());
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(record.getAnnotations())
                .setTime(record.getTime())
                .setDimensions(ImmutableMap.copyOf(effectiveDimensions))
                .build();

        final Source injectingSource = _sourceBuilder.setReplaceExisting(false).build();
        injectingSource.attach(_mockObserver);
        notify(_mockSource, record);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(injectingSource), argument.capture());
        final Record actualRecord = argument.getValue();

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
    private DimensionInjectingSource.Builder _sourceBuilder;
    private ImmutableMap<String, String> _dimensions;

    private static final Observable OBSERVABLE = new Observable() {
        @Override
        public void attach(final Observer observer) {
        }

        @Override
        public void detach(final Observer observer) {
        }
    };
}
