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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Interface for classes which create <code>DynamicConfiguration</code>
 * instances. The factory enables dynamic configurations where the definition
 * of the dynamic configuration, namely its source builders and triggers vary
 * based on data available at runtime.
 *
 * This is accomplished by creating a new dynamic configuration instance. The
 * specific sources and triggers included in the dynamic configuration are left
 * up to the specific implementation of this interface as is the interpretation
 * or mapping of keys.
 *
 * The builder is supplied to the factory's create method in order to allow the
 * caller to set any non-source/trigger specific fields on the builder prior to
 * creation of the new dynamic configuration instance.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
public interface DynamicConfigurationFactory {

    /**
     * Create a new <code>DynamicConfiguration</code> from the specified builder
     * updated with the specified keys using the strategy defined by the specific
     * implementation.
     *
     * @param builder The <code>Builder</code> for <code>DynamicConfiguration</code>.
     * @param keys The <code>Collection</code> of <code>Key</code> instances.
     * @return New instance of <code>DynamicConfiguration</code>.
     */
    DynamicConfiguration create(
            final DynamicConfiguration.Builder builder,
            final Collection<Key> keys);

    /**
     * Update a <code>DynamicConfiguration$Builder</code> with the specified keys using
     * the strategy defined by the specific implementation.
     *
     * @param builder The <code>Builder</code> for <code>DynamicConfiguration</code>.
     * @param keys The <code>Collection</code> of <code>Key</code> instances.
     */
    void update(
            final DynamicConfiguration.Builder builder,
            final Collection<Key> keys);

    /**
     * Key descriptor.
     */
    final class Key {

        /**
         * Public constructor for key with one part. Avoids unnecessary Object[] construction.
         *
         * @param part The single key part.
         */
        public Key(final String part) {
            _parts = ImmutableList.of(part);
        }

        /**
         * Public constructor for key with two parts. Avoids unnecessary Object[] construction.
         *
         * @param firstPart The first key part.
         * @param secondPart The second key part.
         */
        public Key(final String firstPart, final String secondPart) {
            _parts = ImmutableList.of(firstPart, secondPart);
        }

        /**
         * Public constructor from an array or variable number of key parts.
         *
         * @param parts The key parts.
         */
        public Key(final String... parts) {
            _parts = Collections.unmodifiableList(Arrays.asList(parts));
        }

        /**
         * Public constructor from a list of key parts.
         *
         * @param parts The key parts.
         */
        public Key(final List<String> parts) {
            _parts = ImmutableList.copyOf(parts);
        }

        public List<String> getParts() {
            return _parts;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Key)) {
                return false;
            }
            final Key otherKey = (Key) other;
            return this.getParts().equals(otherKey.getParts());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(getParts());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(Key.class)
                    .add("id", Integer.toHexString(System.identityHashCode(this)))
                    .add("Parts", _parts)
                    .toString();
        }

        private final List<String> _parts;
    }
}
