/*
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
package com.arpnetworking.metrics.common.sources;

import com.arpnetworking.commons.observer.Observer;
import net.sf.oval.exception.ConstraintsViolatedException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the BaseSource class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class BaseSourceTest {

    @Test
    public void testAttachDetachNotify() {
        final TestSource source = new TestSource.Builder()
                .setName("My name")
                .build();
        final Object event = new Object();
        final Observer observer1 = Mockito.mock(Observer.class, "observer1");
        final Observer observer2 = Mockito.mock(Observer.class, "observer2");

        source.publicNotify(event);
        Mockito.verifyZeroInteractions(observer1);
        Mockito.verifyZeroInteractions(observer2);
        Mockito.reset(observer1);
        Mockito.reset(observer2);

        source.attach(observer1);
        source.publicNotify(event);
        Mockito.verify(observer1).notify(source, event);
        Mockito.verifyZeroInteractions(observer2);
        Mockito.reset(observer1);
        Mockito.reset(observer2);

        source.attach(observer2);
        source.publicNotify(event);
        Mockito.verify(observer1).notify(source, event);
        Mockito.verify(observer2).notify(source, event);
        Mockito.reset(observer1);
        Mockito.reset(observer2);

        source.detach(observer2);
        source.publicNotify(event);
        Mockito.verify(observer1).notify(source, event);
        Mockito.verifyZeroInteractions(observer2);
        Mockito.reset(observer1);
        Mockito.reset(observer2);
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testBuilderEmptyName() {
        new TestSource.Builder()
                .setName("")
                .build();
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testBuilderNullName() {
        new TestSource.Builder()
                .setName(null)
                .build();
    }

    @Test
    public void testGetName() {
        final String expectedName = "My name";
        final BaseSource source = new TestSource.Builder()
                .setName(expectedName)
                .build();
        Assert.assertEquals(expectedName, source.getName());
    }

    @Test
    public void testGetMetricSafeName() {
        final String name = "My name/with unsafe.characters";
        final String expectedName = "My_name_with_unsafe_characters";
        final BaseSource source = new TestSource.Builder()
                .setName(name)
                .build();
        Assert.assertEquals(expectedName, source.getMetricSafeName());
    }

    @Test
    public void testToString() {
        final String asString = new TestSource.Builder()
                .setName("My name")
                .build()
                .toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private static final class TestSource extends BaseSource {

        @Override
        public void start() {
            // Do nothing
        }

        @Override
        public void stop() {
            // Do nothing
        }

        public void publicNotify(final Object event) {
            super.notify(event);
        }

        private TestSource(final Builder builder) {
            super(builder);
        }

        private static final class Builder extends BaseSource.Builder<Builder, TestSource> {

            @Override
            protected Builder self() {
                return this;
            }

            private Builder() {
                super(TestSource::new);
            }
        }
    }
}
