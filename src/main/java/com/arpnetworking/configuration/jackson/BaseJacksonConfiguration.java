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
package com.arpnetworking.configuration.jackson;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.configuration.Configuration;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Optional;
import net.sf.oval.constraint.NotNull;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.function.Function;

/**
 * Common base class for <code>Configuration</code> implementations based on
 * Jackson's <code>ObjectMapper</code>.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public abstract class BaseJacksonConfiguration extends com.arpnetworking.configuration.BaseConfiguration {

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getProperty(final String name) {
        final Optional<JsonNode> jsonNode = getJsonSource().getValue(name.split("\\."));
        if (jsonNode.isPresent()) {
            return Optional.fromNullable(jsonNode.get().asText());
        }
        return Optional.absent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> getPropertyAs(final String name, final Class<? extends T> clazz) throws IllegalArgumentException {
        final Optional<String> property = getProperty(name);
        if (!property.isPresent()) {
            return Optional.absent();
        }
        try {
            return Optional.fromNullable(_objectMapper.readValue(property.get(), clazz));
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to construct object from configuration; name=%s, class=%s, property=%s",
                            name,
                            clazz,
                            property),
                    e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> getAs(final Class<? extends T> clazz) throws IllegalArgumentException {
        final Optional<JsonNode> property = getJsonSource().getValue();
        if (!property.isPresent()) {
            return Optional.absent();
        }
        try {
            return Optional.fromNullable(_objectMapper.treeToValue(property.get(), clazz));
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to construct object from configuration; class=%s, property=%s",
                            clazz,
                            property),
                    e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> getPropertyAs(final String name, final Type type) throws IllegalArgumentException {
        final Optional<String> property = getProperty(name);
        if (!property.isPresent()) {
            return Optional.absent();
        }
        try {
            final TypeFactory typeFactory = _objectMapper.getTypeFactory();
            @SuppressWarnings("unchecked")
            final Optional<T> value = Optional.fromNullable((T) _objectMapper.readValue(
                    property.get(),
                    typeFactory.constructType(type)));
            return value;
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to construct object from configuration; name=%s, type=%s, property=%s",
                            name,
                            type,
                            property),
                    e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> getAs(final Type type) throws IllegalArgumentException {
        final Optional<JsonNode> property = getJsonSource().getValue();
        if (!property.isPresent()) {
            return Optional.absent();
        }
        try {
            final TypeFactory typeFactory = _objectMapper.getTypeFactory();
            @SuppressWarnings("unchecked")
            final Optional<T> value = Optional.fromNullable((T) _objectMapper.readValue(
                    _objectMapper.treeAsTokens(property.get()), typeFactory.constructType(type)));
            return value;
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to construct object from configuration; type=%s, property=%s",
                            type,
                            property),
                    e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("ObjectMapper", _objectMapper)
                .build();
    }

    /**
     * Accessor for active root <code>JsonSource</code> instance.
     *
     * @return Instance of <code>JsonSource</code>.
     */
    protected abstract JsonNodeSource getJsonSource();

    /**
     * Protected constructor.
     *
     * @param builder The <code>Builder</code> instance to construct from.
     */
    protected BaseJacksonConfiguration(final Builder<?, ?> builder) {
        _objectMapper = builder._objectMapper;
    }

    protected final ObjectMapper _objectMapper;

    /**
     * Builder for <code>BaseObjectMapperConfiguration</code>.
     *
     * @param <T> type of the builder
     * @param <S> type of the object to be built
     */
    protected abstract static class Builder<T extends Builder<?, ?>, S extends Configuration> extends OvalBuilder<S> {

        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor The constructor for the concrete type to be created by this builder.
         */
        @SuppressWarnings(value = {"rawtypes", "unchecked"})
        protected Builder(final Function<T, S> targetConstructor) {
            super((Function<? extends Builder, S>) targetConstructor);
        }

        /**
         * Set the <code>ObjectMapper</code> instance. Optional. Default is
         * created by <code>ObjectMapperFactory</code>. Cannot be null.
         *
         * @param value The <code>ObjectMapper</code> instance.
         * @return This <code>Builder</code> instance.
         */
        public T setObjectMapper(final ObjectMapper value) {
            _objectMapper = value;
            return self();
        }

        /**
         * Called by setters to always return appropriate subclass of
         * <code>Builder</code>, even from setters of base class.
         *
         * @return instance with correct <code>Builder</code> class type.
         */
        protected abstract T self();

        @NotNull
        private ObjectMapper _objectMapper = ObjectMapperFactory.getInstance();
    }
}
