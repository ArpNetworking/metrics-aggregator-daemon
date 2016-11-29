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
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.oval.constraint.NotNull;

import java.util.Optional;
import java.util.function.Function;

/**
 * Abstract base class for <code>JsonSource</code> implementations.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public abstract class BaseJsonNodeSource implements JsonNodeSource {

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("objectMapper", _objectMapper)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /**
     * Find the <code>JsonNode</code> if one exists from a specified root node
     * given a sequence of keys to look-up.
     *
     * @param node The root <code>JsonNode</code>.
     * @param keys The sequence of keys to search for.
     * @return The <code>Optional</code> <code>JsonNode</code> instance.
     */
    protected static Optional<JsonNode> getValue(final Optional<JsonNode> node, final String... keys) {
        JsonNode jsonNode = node.orElse(null);
        for (final String key : keys) {
            if (jsonNode == null) {
                break;
            }
            jsonNode = jsonNode.get(key);
        }
        return Optional.ofNullable(jsonNode);
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected BaseJsonNodeSource(final Builder<?, ?> builder) {
        _objectMapper = builder._objectMapper;
    }

    protected final ObjectMapper _objectMapper;

    /**
     * Builder for <code>BaseJsonNodeSource</code>.
     *
     * @param <T> type of the builder
     * @param <S> type of the object to be built
     */
    protected abstract static class Builder<T extends Builder<?, ?>, S extends JsonNodeSource> extends OvalBuilder<S> {

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
        protected ObjectMapper _objectMapper = ObjectMapperFactory.getInstance();
    }
}
