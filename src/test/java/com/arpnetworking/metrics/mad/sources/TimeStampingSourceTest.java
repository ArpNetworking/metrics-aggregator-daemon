/*
 * Copyright 2019 Dropbox.com
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Unit tests for the {@link TimeStampingSource}.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class TimeStampingSourceTest {
    private TimeStampingSource _source;
    private Source _mockSource;
    private Observer _mockObserver;
    private ArgumentCaptor<Record> _captor = ArgumentCaptor.forClass(Record.class);

    @Before
    public void setUp() {
        _mockSource = Mockito.mock(Source.class);
        _source = new TimeStampingSource.Builder()
                .setName("TimeStampingSource")
                .setSource(_mockSource)
                .build();

        _mockObserver = Mockito.mock(Observer.class);
        _source.attach(_mockObserver);
    }

    @Test
    public void testAttachStampingObserver() {
        Mockito.verify(_mockSource).attach(Mockito.any(Observer.class));
    }

    @Test
    public void testStart() {
        _source.start();
        Mockito.verify(_mockSource).start();
    }

    @Test
    public void testStop() {
        _source.stop();
        Mockito.verify(_mockSource).stop();
    }

    @Test
    public void testToString() {
        final String asString = _source.toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testRestamp() {
        final ZonedDateTime before = ZonedDateTime.now();

        final Record record = TestBeanFactory.createRecordBuilder()
                .setTime(ZonedDateTime.of(2019, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")))
                .build();
        notify(_mockSource, record);

        Mockito.verify(_mockObserver).notify(Mockito.same(_source), _captor.capture());
        final Record received = _captor.getValue();

        final ZonedDateTime after = ZonedDateTime.now();

        Assert.assertTrue(String.format("Timestamp should have been between %s and %s but was %s", before.toString(),
                after.toString(), received.getTime().toString()),
                before.compareTo(received.getTime()) <= 0 && received.getTime().compareTo(after) <= 0);
    }

    private static void notify(final Observable observable, final Object event) {
        final ArgumentCaptor<Observer> argument = ArgumentCaptor.forClass(Observer.class);
        Mockito.verify(observable).attach(argument.capture());
        for (final Observer observer : argument.getAllValues()) {
            observer.notify(observable, event);
        }
    }
}
