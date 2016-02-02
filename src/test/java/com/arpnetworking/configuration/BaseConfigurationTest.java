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
package com.arpnetworking.configuration;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Tests for the <code>BaseConfiguration</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class BaseConfigurationTest {

    @Test
    public void testGetPropertyWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        final String value = configuration.getProperty("foo", "default");
        Assert.assertEquals("bar", value);
    }

    @Test
    public void testGetPropertyWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final String value = configuration.getProperty("foo", "default");
        Assert.assertEquals("default", value);
    }

    @Test
    public void testGetRequiredPropertyExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        final String value = configuration.getRequiredProperty("foo");
        Assert.assertEquals("bar", value);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyMissing() {
        TestConfiguration.create().getRequiredProperty("foo");
    }

    @Test
    public void testGetPropertyAsBooleanExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "false");
        final Optional<Boolean> value = configuration.getPropertyAsBoolean("foo");
        Assert.assertTrue(value.isPresent());
        Assert.assertFalse(value.get());
    }

    @Test
    public void testGetPropertyAsBooleanMissing() {
        final Configuration configuration = TestConfiguration.create();
        final Optional<Boolean> value = configuration.getPropertyAsBoolean("foo");
        Assert.assertFalse(value.isPresent());
    }

    @Test
    public void testGetPropertyAsBooleanWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "false");
        final boolean value = configuration.getPropertyAsBoolean("foo", true);
        Assert.assertFalse(value);
    }

    @Test
    public void testGetPropertyAsBooleanWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final boolean value = configuration.getPropertyAsBoolean("foo", true);
        Assert.assertTrue(value);
    }

    @Test
    public void testGetRequiredPropertyAsBooleanExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "false");
        final boolean value = configuration.getRequiredPropertyAsBoolean("foo");
        Assert.assertFalse(value);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsBooleanMissing() {
        TestConfiguration.create().getRequiredPropertyAsBoolean("foo");
    }

    @Test
    public void testGetPropertyAsShortExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final Optional<Short> value = configuration.getPropertyAsShort("foo");
        Assert.assertTrue(value.isPresent());
        Assert.assertEquals(1, value.get().shortValue());
    }

    @Test
    public void testGetPropertyAsShortMissing() {
        final Configuration configuration = TestConfiguration.create();
        final Optional<Short> value = configuration.getPropertyAsShort("foo");
        Assert.assertFalse(value.isPresent());
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsShortInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsShort("foo");
    }

    @Test
    public void testGetPropertyAsShortWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final short value = configuration.getPropertyAsShort("foo", (short) 2);
        Assert.assertEquals(1, value);
    }

    @Test
    public void testGetPropertyAsShortWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final short value = configuration.getPropertyAsShort("foo", (short) 2);
        Assert.assertEquals(2, value);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsShortWithDefaultInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsShort("foo", (short) 2);
    }

    @Test
    public void testGetRequiredPropertyAsShortExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final short value = configuration.getRequiredPropertyAsShort("foo");
        Assert.assertEquals(1, value);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsShortMissing() {
        TestConfiguration.create().getRequiredPropertyAsShort("foo");
    }

    @Test(expected = NumberFormatException.class)
    public void testGetRequiredPropertyAsShortInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getRequiredPropertyAsShort("foo");
    }

    @Test
    public void testGetPropertyAsIntegerExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final Optional<Integer> value = configuration.getPropertyAsInteger("foo");
        Assert.assertTrue(value.isPresent());
        Assert.assertEquals(1L, value.get().longValue());
    }

    @Test
    public void testGetPropertyAsIntegerMissing() {
        final Configuration configuration = TestConfiguration.create();
        final Optional<Integer> value = configuration.getPropertyAsInteger("foo");
        Assert.assertFalse(value.isPresent());
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsIntegerInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsInteger("foo");
    }

    @Test
    public void testGetPropertyAsIntegerWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final int value = configuration.getPropertyAsInteger("foo", 2);
        Assert.assertEquals(1, value);
    }

    @Test
    public void testGetPropertyAsIntegerWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final int value = configuration.getPropertyAsInteger("foo", 2);
        Assert.assertEquals(2, value);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsIntegerWithDefaultInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsInteger("foo", 2);
    }

    @Test
    public void testGetRequiredPropertyAsIntegerExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final int value = configuration.getRequiredPropertyAsInteger("foo");
        Assert.assertEquals(1, value);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsIntegerMissing() {
        TestConfiguration.create().getRequiredPropertyAsInteger("foo");
    }

    @Test(expected = NumberFormatException.class)
    public void testGetRequiredPropertyAsIntegerInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getRequiredPropertyAsInteger("foo");
    }

    @Test
    public void testGetPropertyAsLongExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final Optional<Long> value = configuration.getPropertyAsLong("foo");
        Assert.assertTrue(value.isPresent());
        Assert.assertEquals(1L, value.get().longValue());
    }

    @Test
    public void testGetPropertyAsLongMissing() {
        final Configuration configuration = TestConfiguration.create();
        final Optional<Long> value = configuration.getPropertyAsLong("foo");
        Assert.assertFalse(value.isPresent());
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsLongInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsLong("foo");
    }

    @Test
    public void testGetPropertyAsLongWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final long value = configuration.getPropertyAsLong("foo", 2);
        Assert.assertEquals(1, value);
    }

    @Test
    public void testGetPropertyAsLongWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final long value = configuration.getPropertyAsLong("foo", 2);
        Assert.assertEquals(2, value);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsLongWithDefaultInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsLong("foo", 2);
    }

    @Test
    public void testGetRequiredPropertyAsLongExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1");
        final long value = configuration.getRequiredPropertyAsLong("foo");
        Assert.assertEquals(1, value);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsLongMissing() {
        TestConfiguration.create().getRequiredPropertyAsLong("foo");
    }

    @Test(expected = NumberFormatException.class)
    public void testGetRequiredPropertyAsLongInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getRequiredPropertyAsLong("foo");
    }

    @Test
    public void testGetPropertyAsFloatExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1.23");
        final Optional<Float> value = configuration.getPropertyAsFloat("foo");
        Assert.assertTrue(value.isPresent());
        Assert.assertEquals(1.23f, value.get(), 0.001);
    }

    @Test
    public void testGetPropertyAsFloatMissing() {
        final Configuration configuration = TestConfiguration.create();
        final Optional<Float> value = configuration.getPropertyAsFloat("foo");
        Assert.assertFalse(value.isPresent());
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsFloatInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsFloat("foo");
    }

    @Test
    public void testGetPropertyAsFloatWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1.23");
        final float value = configuration.getPropertyAsFloat("foo", 2.46f);
        Assert.assertEquals(1.23f, value, 0.001);
    }

    @Test
    public void testGetPropertyAsFloatWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final float value = configuration.getPropertyAsFloat("foo", 2.46f);
        Assert.assertEquals(2.46f, value, 0.001);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsFloatWithDefaultInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsFloat("foo", 2.46f);
    }

    @Test
    public void testGetRequiredPropertyAsFloatExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1.23");
        final float value = configuration.getRequiredPropertyAsFloat("foo");
        Assert.assertEquals(1.23f, value, 0.001);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsFloatMissing() {
        TestConfiguration.create().getRequiredPropertyAsFloat("foo");
    }

    @Test(expected = NumberFormatException.class)
    public void testGetRequiredPropertyAsFloatInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getRequiredPropertyAsFloat("foo");
    }

    @Test
    public void testGetPropertyAsDoubleExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1.23");
        final Optional<Double> value = configuration.getPropertyAsDouble("foo");
        Assert.assertTrue(value.isPresent());
        Assert.assertEquals(1.23d, value.get(), 0.001);
    }

    @Test
    public void testGetPropertyAsDoubleMissing() {
        final Configuration configuration = TestConfiguration.create();
        final Optional<Double> value = configuration.getPropertyAsDouble("foo");
        Assert.assertFalse(value.isPresent());
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsDoubleInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsDouble("foo");
    }

    @Test
    public void testGetPropertyAsDoubleWithDefaultExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1.23");
        final double value = configuration.getPropertyAsDouble("foo", 2.46d);
        Assert.assertEquals(1.23d, value, 0.001);
    }

    @Test
    public void testGetPropertyAsDoubleWithDefaultMissing() {
        final Configuration configuration = TestConfiguration.create();
        final double value = configuration.getPropertyAsDouble("foo", 2.46d);
        Assert.assertEquals(2.46d, value, 0.001);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetPropertyAsDoubleWithDefaultInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getPropertyAsDouble("foo", 2.46d);
    }

    @Test
    public void testGetRequiredPropertyAsDoubleExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "1.23");
        final double value = configuration.getRequiredPropertyAsDouble("foo");
        Assert.assertEquals(1.23d, value, 0.001);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsDoubleMissing() {
        TestConfiguration.create().getRequiredPropertyAsDouble("foo");
    }

    @Test(expected = NumberFormatException.class)
    public void testGetRequiredPropertyAsDoubleInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "ABC");
        configuration.getRequiredPropertyAsDouble("foo");
    }

    @Test
    public void testGetPropertyAsExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        final String value = configuration.getPropertyAs("foo", String.class, "default");
        Assert.assertEquals("bar", value);
    }

    @Test
    public void testGetPropertyAsMissing() {
        final Configuration configuration = TestConfiguration.create();
        final String value = configuration.getPropertyAs("foo", String.class, "default");
        Assert.assertEquals("default", value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPropertyAsInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        configuration.getPropertyAs("foo", Map.class, "default");
    }

    @Test
    public void testGetRequiredPropertyAsExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        final String value = configuration.getRequiredPropertyAs("foo", String.class);
        Assert.assertEquals("bar", value);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredPropertyAsMissing() {
        final Configuration configuration = TestConfiguration.create();
        configuration.getRequiredPropertyAs("foo", String.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetRequiredPropertyAsInvalid() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        configuration.getRequiredPropertyAs("foo", Map.class);
    }

    @Test
    public void testGetAsExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        @SuppressWarnings("unchecked")
        final Map<String, String> value = (Map<String, String>) configuration.getAs(Map.class, Collections.emptyMap());
        Assert.assertEquals(1, value.size());
        Assert.assertEquals("bar", value.get("foo"));
    }

    @Test
    public void testGetAsMissing() {
        final Configuration configuration = TestConfiguration.createNull();
        @SuppressWarnings("unchecked")
        final Map<String, String> value = (Map<String, String>) configuration.getAs(Map.class, Collections.emptyMap());
        Assert.assertTrue(value.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAsInvalid() {
        final Configuration configuration = TestConfiguration.create();
        configuration.getAs(String.class, Collections.emptyMap());
    }

    @Test
    public void testGetRequiredAsExists() {
        final Configuration configuration = TestConfiguration.create(
                "foo", "bar");
        @SuppressWarnings("unchecked")
        final Map<String, String> value = (Map<String, String>) configuration.getRequiredAs(Map.class);
        Assert.assertEquals(1, value.size());
        Assert.assertEquals("bar", value.get("foo"));
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetRequiredAsMissing() {
        final Configuration configuration = TestConfiguration.createNull();
        configuration.getRequiredAs(Map.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetRequiredAsInvalid() {
        final Configuration configuration = TestConfiguration.create();
        configuration.getRequiredAs(String.class);
    }

    private static final class TestConfiguration extends BaseConfiguration {

        public static TestConfiguration create(final Object... keysAndValues) {
            final Map<String, String> configuration = Maps.newHashMap();
            String key = null;
            for (final Object keyOrValue : keysAndValues) {
                if (key == null) {
                    key = (String) keyOrValue;
                } else {
                    configuration.put(key, keyOrValue.toString());
                    key = null;
                }
            }
            return new TestConfiguration(configuration);
        }

        public static TestConfiguration createNull() {
            return new TestConfiguration(null);
        }

        @Override
        public Optional<String> getProperty(final String name) {
            return Optional.fromNullable(_configuration.get(name));
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getPropertyAs(final String name, final Class<? extends T> clazz) throws IllegalArgumentException {
            final Object value = _configuration.get(name);
            if (value == null) {
                return Optional.absent();
            }
            if (!clazz.isInstance(value)) {
                throw new IllegalArgumentException("Cannot convert to: " + clazz);
            }
            return Optional.fromNullable((T) value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getPropertyAs(final String name, final Type type) throws IllegalArgumentException {
            return (Optional<T>) getPropertyAs(name, type.getClass());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getAs(final Class<? extends T> clazz) throws IllegalArgumentException {
            if (_configuration == null) {
                return Optional.absent();
            }
            if (!clazz.isInstance(_configuration)) {
                throw new IllegalArgumentException("Cannot convert to: " + clazz);
            }
            return Optional.of((T) _configuration);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getAs(final Type type) throws IllegalArgumentException {
            return (Optional<T>) getAs(type.getClass());
        }

        private TestConfiguration(final Map<String, String> configuration) {
            _configuration = configuration;
        }

        private final Map<String, String> _configuration;
    }
}
