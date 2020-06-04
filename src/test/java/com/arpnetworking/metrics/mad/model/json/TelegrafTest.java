/*
 * Copyright 2020 Inscope Metrics
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
package com.arpnetworking.metrics.mad.model.json;

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.EqualityTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Tests for the {@link Telegraf} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class TelegrafTest {

    private final Supplier<Telegraf.Builder> _telegrafBuilder = () -> new Telegraf.Builder()
            .setTimestamp(System.currentTimeMillis())
            .setName("test")
            .setFields(ImmutableMap.of("metric", "30"))
            .setTags(ImmutableMap.of("host", "localhost"));

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _telegrafBuilder.get(),
                Telegraf.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_telegrafBuilder.get());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualityTestHelper.testEquality(
                _telegrafBuilder.get(),
                new Telegraf.Builder()
                        .setTimestamp(System.currentTimeMillis() + 1000)
                        .setName("test2")
                        .setFields(ImmutableMap.of("metric2", "60"))
                        .setTags(ImmutableMap.of("host", "localhost2")),
                Telegraf.class);
    }

    @Test
    public void testToString() {
        final String asString = _telegrafBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
