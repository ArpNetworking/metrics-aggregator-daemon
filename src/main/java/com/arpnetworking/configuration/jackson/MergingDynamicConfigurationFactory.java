/**
 * Copyright 2015 Groupon.com
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
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * Implementation of <code>DynamicConfigurationFactory</code> which maps keys
 * to zero or more subordinate <code>DynamicConfigurationFactory</code> instances.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class MergingDynamicConfigurationFactory implements DynamicConfigurationFactory {

    @Override
    public DynamicConfiguration create(
            final DynamicConfiguration.Builder builder,
            final Collection<Key> keys) {
        update(builder, keys);
        return builder.build();
    }

    @Override
    public void update(
            final DynamicConfiguration.Builder builder,
            final Collection<Key> keys) {
        for (final DynamicConfigurationFactory factory : _factories) {
            factory.update(builder, keys);
        }
    }


    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("factories", _factories)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private MergingDynamicConfigurationFactory(final Builder builder) {
        _factories = Lists.newArrayList(builder._factories);
    }

    private final List<DynamicConfigurationFactory> _factories;

    /**
     * <code>Builder</code> implementation for <code>MergingDynamicConfigurationFactory</code>.
     */
    public static final class Builder extends OvalBuilder<MergingDynamicConfigurationFactory> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(MergingDynamicConfigurationFactory::new);
        }

        /**
         * Set the factories.
         *
         * @param value The factories.
         * @return This <code>Builder</code> instance.
         */
        public Builder setFactories(final List<DynamicConfigurationFactory> value) {
            _factories = value;
            return this;
        }

        private List<DynamicConfigurationFactory> _factories;
    }
}
