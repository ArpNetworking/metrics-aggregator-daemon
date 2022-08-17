/*
 * Copyright 2020 Dropbox
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
package com.arpnetworking.metrics.mad;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.metrics.Event;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.impl.ApacheHttpSink;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link Main} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class MainTest {

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    @Test
    public void testCreateSinks() throws IOException {
        final JsonNode apacheSinkNode = OBJECT_MAPPER.readTree(
                "{\"class\":\"com.arpnetworking.metrics.impl.ApacheHttpSink\"}");
        final JsonNode testSinkNode = OBJECT_MAPPER.readTree(
                "{\"class\":\"com.arpnetworking.metrics.mad.MainTest$TestSink\"}");
        final List<Sink> sinks = Main.createSinks(ImmutableList.of(apacheSinkNode, testSinkNode));

        assertEquals(2, sinks.size());
        MatcherAssert.assertThat(sinks.get(0), Matchers.instanceOf(ApacheHttpSink.class));
        MatcherAssert.assertThat(sinks.get(1), Matchers.instanceOf(TestSink.class));
    }

    @Test
    public void testSetSinkProperties() throws IOException {
        final JsonNode testSinkNode = OBJECT_MAPPER.readTree(
                "{\"class\":\"com.arpnetworking.metrics.mad.MainTest$TestSink\","
                + "\"foo\":\"bar\"}");
        final List<Sink> sinks = Main.createSinks(ImmutableList.of(testSinkNode));

        assertEquals(1, sinks.size());
        MatcherAssert.assertThat(sinks.get(0), Matchers.instanceOf(TestSink.class));

        final TestSink testSink = (TestSink) sinks.get(0);
        assertEquals("bar", testSink.getFoo());
    }

    @Test(expected = RuntimeException.class)
    public void testSinkDoesNotExist() throws IOException {
        final JsonNode testSinkNode = OBJECT_MAPPER.readTree(
                "{\"class\":\"foo.example.com.does.not.exist\"}");
        Main.createSinks(ImmutableList.of(testSinkNode));
    }

    /**
     * Test sink implementation.
     */
    public static final class TestSink implements Sink {

        private final String _foo;

        public TestSink(final Builder builder) {
            _foo = builder._foo;
        }

        public String getFoo() {
            return _foo;
        }

        @Override
        public void record(final Event event) {
            // Nothing to do
        }

        /**
         * Builder implementation for {@link TestSink} without the
         * Commons Builder pattern.
         */
        public static final class Builder {

            private String _foo;

            public Sink build() {
                return new TestSink(this);
            }

            public Builder setFoo(final String value) {
                _foo = value;
                return this;
            }
        }
    }
}
