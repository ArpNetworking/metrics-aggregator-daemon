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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.test.BuildableTestHelper;
import com.arpnetworking.commons.test.EqualityTestHelper;
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.arpnetworking.test.TestBeanFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Tests for the {@link CalculatedValue} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class CalculatedValueTest {

    private final Supplier<CalculatedValue.Builder<Object>> _calculatedValueBuilder = () -> new CalculatedValue.Builder<>()
            .setValue(TestBeanFactory.createSample())
            .setData(new Object());

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _calculatedValueBuilder.get(),
                CalculatedValue.class);
    }

    @Test
    public void testReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_calculatedValueBuilder.get());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualityTestHelper.testEquality(
                _calculatedValueBuilder.get(),
                new CalculatedValue.Builder<>()
                        .setValue(TestBeanFactory.createSample())
                        .setData(new Object()),
                CalculatedValue.class);
    }

    @Test
    public void testToString() {
        final String asString = _calculatedValueBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
