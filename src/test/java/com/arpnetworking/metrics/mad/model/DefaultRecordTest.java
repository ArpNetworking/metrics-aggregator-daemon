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
package com.arpnetworking.metrics.mad.model;

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.arpnetworking.test.TestBeanFactory;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Tests for the {@link DefaultRecord} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class DefaultRecordTest {

    private final Supplier<DefaultRecord.Builder> _defaultRecordBuilder = () -> new DefaultRecord.Builder()
            .setTime(ZonedDateTime.now())
            .setId(UUID.randomUUID().toString())
            .setDimensions(ImmutableMap.of("dKey", "dValue"))
            .setAnnotations(ImmutableMap.of("aKey", "aValue"))
            .setMetrics(ImmutableMap.of(
                    "metric",
                    TestBeanFactory.createMetric()));

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _defaultRecordBuilder.get(),
                Record.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_defaultRecordBuilder.get());
    }

    @Test
    public void testToString() {
        final String asString = _defaultRecordBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
