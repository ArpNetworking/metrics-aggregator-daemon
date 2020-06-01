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
import com.arpnetworking.commons.test.ThreadLocalBuildableTestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Tests for the {@link Version2g} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class Version2GTest {

    private final Supplier<Version2g.Sample.Unit.Builder> _unitBuilder = () -> new Version2g.Sample.Unit.Builder()
            .setNumerators(ImmutableList.of(
                    new Version2g.CompositeUnit(
                            Version2g.CompositeUnit.Scale.ONE,
                            Version2g.CompositeUnit.Type.BYTE)))
            .setDenominators(ImmutableList.of(
                    new Version2g.CompositeUnit(
                            Version2g.CompositeUnit.Scale.ONE,
                            Version2g.CompositeUnit.Type.SECOND)));

    private final Supplier<Version2g.Sample.Builder> _sampleBuilder = () -> new Version2g.Sample.Builder()
            .setUnit(_unitBuilder.get().build())
            .setValue(1.0);

    private final Supplier<Version2g.Element.Builder> _elementBuilder = () -> new Version2g.Element.Builder()
            .setValues(ImmutableList.of(_sampleBuilder.get().build()));

    private final Supplier<Version2g.Builder> _version2GBuilder = () -> new Version2g.Builder()
            .setAnnotations(ImmutableMap.of("aKey", "aValue"))
            .setCounters(ImmutableMap.of("counter", _elementBuilder.get().build()))
            .setDimensions(ImmutableMap.of("dKey", "dValue"))
            .setEnd(ZonedDateTime.now().plusMinutes(1))
            .setGauges(ImmutableMap.of("gauge", _elementBuilder.get().build()))
            .setId(UUID.randomUUID().toString())
            .setStart(ZonedDateTime.now())
            .setTimers(ImmutableMap.of("timer", _elementBuilder.get().build()))
            .setVersion("2g");

    @Test
    public void testVersion2gBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _version2GBuilder.get(),
                Version2g.class);
    }

    @Test
    public void testVersion2gReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_version2GBuilder.get());
    }

    @Test
    public void testVersion2gToString() {
        final String asString = _version2GBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testElementBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _elementBuilder.get(),
                Version2g.Element.class);
    }

    @Test
    public void testElementgReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_elementBuilder.get());
    }

    @Test
    public void testElementToString() {
        final String asString = _elementBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testSampleBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _sampleBuilder.get(),
                Version2g.Sample.class);
    }

    @Test
    public void testSampleReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_sampleBuilder.get());
    }

    @Test
    public void testSampleToString() {
        final String asString = _sampleBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testUnitBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _unitBuilder.get(),
                Version2g.Sample.Unit.class);
    }

    @Test
    public void testUnitReset() throws Exception {
        ThreadLocalBuildableTestHelper.testReset(_unitBuilder.get());
    }

    @Test
    public void testUnitToString() {
        final String asString = _unitBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
