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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogReferenceOnly;
import com.google.common.base.Optional;

import java.lang.reflect.Type;
import java.util.NoSuchElementException;

/**
 * Abstract base class for <code>Configuration</code> implementations which
 * implements the shared convenience methods which rely core methods. The
 * core methods are left for implementation by each concrete subclass.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public abstract class BaseConfiguration implements Configuration {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProperty(final String name, final String defaultValue) {
        final Optional<String> optional = getProperty(name);
        if (optional.isPresent()) {
            return optional.get();
        }
        return defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRequiredProperty(final String name) throws NoSuchElementException {
        final Optional<String> optional = getProperty(name);
        if (optional.isPresent()) {
            return optional.get();
        }
        throw new NoSuchElementException(
                String.format("Required configuration property does not exist; name=%s", name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Boolean> getPropertyAsBoolean(final String name) {
        final Optional<String> property = getProperty(name);
        return property.isPresent() ? Optional.of(Boolean.parseBoolean(property.get())) : Optional.<Boolean>absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getPropertyAsBoolean(final String name, final boolean defaultValue) {
        final Optional<Boolean> property = getPropertyAsBoolean(name);
        return property.or(defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getRequiredPropertyAsBoolean(final String name) throws NoSuchElementException {
        final String property = getRequiredProperty(name);
        return Boolean.parseBoolean(property);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getPropertyAsInteger(final String name) throws NumberFormatException {
        final Optional<String> property = getProperty(name);
        return property.isPresent() ? Optional.of(Integer.parseInt(property.get())) : Optional.<Integer>absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPropertyAsInteger(final String name, final int defaultValue) throws NumberFormatException {
        final Optional<Integer> property = getPropertyAsInteger(name);
        return property.or(defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRequiredPropertyAsInteger(final String name) throws NoSuchElementException, NumberFormatException {
        final String property = getRequiredProperty(name);
        return Integer.parseInt(property);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getPropertyAsLong(final String name) throws NumberFormatException {
        final Optional<String> property = getProperty(name);
        return property.isPresent() ? Optional.of(Long.parseLong(property.get())) : Optional.<Long>absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPropertyAsLong(final String name, final long defaultValue) throws NumberFormatException {
        final Optional<Long> property = getPropertyAsLong(name);
        return property.or(defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getRequiredPropertyAsLong(final String name) throws NoSuchElementException, NumberFormatException {
        final String property = getRequiredProperty(name);
        return Long.parseLong(property);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Double> getPropertyAsDouble(final String name) throws NumberFormatException {
        final Optional<String> property = getProperty(name);
        return property.isPresent() ? Optional.of(Double.parseDouble(property.get())) : Optional.<Double>absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getPropertyAsDouble(final String name, final double defaultValue) throws NumberFormatException {
        final Optional<Double> property = getPropertyAsDouble(name);
        return property.or(defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getRequiredPropertyAsDouble(final String name) throws NoSuchElementException, NumberFormatException {
        final String property = getRequiredProperty(name);
        return Double.parseDouble(property);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Float> getPropertyAsFloat(final String name) throws NumberFormatException {
        final Optional<String> property = getProperty(name);
        return property.isPresent() ? Optional.of(Float.parseFloat(property.get())) : Optional.<Float>absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getPropertyAsFloat(final String name, final float defaultValue) throws NumberFormatException {
        final Optional<Float> property = getPropertyAsFloat(name);
        return property.or(defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getRequiredPropertyAsFloat(final String name) throws NoSuchElementException, NumberFormatException {
        final String property = getRequiredProperty(name);
        return Float.parseFloat(property);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Short> getPropertyAsShort(final String name) throws NumberFormatException {
        final Optional<String> property = getProperty(name);
        return property.isPresent() ? Optional.of(Short.valueOf(property.get())) : Optional.<Short>absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getPropertyAsShort(final String name, final short defaultValue) throws NumberFormatException {
        final Optional<Short> property = getPropertyAsShort(name);
        return property.isPresent() ? property.get().shortValue() : defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getRequiredPropertyAsShort(final String name) throws NoSuchElementException, NumberFormatException {
        final String property = getRequiredProperty(name);
        return Short.parseShort(property);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getPropertyAs(final String name, final Class<? extends T> clazz, final T defaultValue)
            throws IllegalArgumentException {
        final Optional<T> property = getPropertyAs(name, clazz);
        return property.isPresent() ? property.get() : defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getRequiredPropertyAs(final String name, final Class<? extends T> clazz)
            throws NoSuchElementException, IllegalArgumentException {
        final Optional<T> property = getPropertyAs(name, clazz);
        if (!property.isPresent()) {
            throw new NoSuchElementException(
                    String.format("Required configuration property does not exist; name=%s", name));
        }
        return property.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(final Class<? extends T> clazz, final T defaultValue) throws IllegalArgumentException {
        final Optional<T> property = getAs(clazz);
        return property.isPresent() ? property.get() : defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getRequiredAs(final Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException {
        final Optional<T> property = getAs(clazz);
        if (!property.isPresent()) {
            throw new NoSuchElementException("Configuration does not exist");
        }
        return property.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getPropertyAs(final String name, final Type type, final T defaultValue)
            throws IllegalArgumentException {
        final Optional<T> property = getPropertyAs(name, type);
        return property.isPresent() ? property.get() : defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getRequiredPropertyAs(final String name, final Type type)
            throws NoSuchElementException, IllegalArgumentException {
        final Optional<T> property = getPropertyAs(name, type);
        if (!property.isPresent()) {
            throw new NoSuchElementException(
                    String.format("Required configuration property does not exist; name=%s", name));
        }
        return property.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(final Type type, final T defaultValue) throws IllegalArgumentException {
        final Optional<T> property = getAs(type);
        return property.isPresent() ? property.get() : defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getRequiredAs(final Type type) throws NoSuchElementException, IllegalArgumentException {
        final Optional<T> property = getAs(type);
        if (!property.isPresent()) {
            throw new NoSuchElementException("Configuration does not exist");
        }
        return property.get();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogReferenceOnly.of(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }
}
