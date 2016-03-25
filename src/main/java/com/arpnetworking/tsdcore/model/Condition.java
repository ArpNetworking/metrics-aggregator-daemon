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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

/**
 * Describes a condition.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
@Loggable
public final class Condition {

    public String getName() {
        return _name;
    }

    public FQDSN getFQDSN() {
        return _fqdsn;
    }

    public Quantity getThreshold() {
        return _threshold;
    }

    public Optional<Boolean> isTriggered() {
        return _triggered;
    }

    public ImmutableMap<String, Object> getExtensions() {
        return _extensions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Name", _name)
                .add("FQDSN", _fqdsn)
                .add("Threshold", _threshold)
                .add("Triggered", _triggered)
                .add("Extension", _extensions)
                .toString();
    }

    private Condition(final Builder builder) {
        _name = builder._name;
        _fqdsn = builder._fqdsn;
        _threshold = builder._threshold;
        _triggered = Optional.fromNullable(builder._triggered);
        _extensions = builder._extensions;
    }

    private final String _name;
    private final FQDSN _fqdsn;
    private final Quantity _threshold;
    private final Optional<Boolean> _triggered;
    private final ImmutableMap<String, Object> _extensions;

    /**
     * <code>Builder</code> implementation for <code>Condition</code>.
     */
    public static final class Builder extends OvalBuilder<Condition> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(Condition::new);
        }

        /**
         * Set the name.
         *
         * @param value The name.
         * @return This <code>Builder</code> instance.
         */
        public Builder setName(final String value) {
            _name = value;
            return this;
        }

        /**
         * Set the fully qualified data space name (FQDSN).
         *
         * @param value The fully qualified data space name (FQDSN).
         * @return This <code>Builder</code> instance.
         */
        public Builder setFQDSN(final FQDSN value) {
            _fqdsn = value;
            return this;
        }

        /**
         * Set the threshold.
         *
         * @param value The threshopld.
         * @return This <code>Builder</code> instance.
         */
        public Builder setThreshold(final Quantity value) {
            _threshold = value;
            return this;
        }

        /**
         * Set the whether it was triggered.
         *
         * @param value Whether it was triggered.
         * @return This <code>Builder</code> instance.
         */
        public Builder setTriggered(final Boolean value) {
            _triggered = value;
            return this;
        }

        /**
         * Set supporting data. Optional. Cannot be null. Default is an empty
         * <code>Map</code>.
         *
         * @param value The supporting data.
         * @return This <code>Builder</code> instance.
         */
        public Builder setExtensions(final ImmutableMap<String, Object> value) {
            _extensions = value;
            return this;
        }

        @NotNull
        @NotEmpty
        private String _name;
        @NotNull
        private FQDSN _fqdsn;
        @NotNull
        private Quantity _threshold;
        private Boolean _triggered;
        @NotNull
        private ImmutableMap<String, Object> _extensions = ImmutableMap.of();
    }
}
