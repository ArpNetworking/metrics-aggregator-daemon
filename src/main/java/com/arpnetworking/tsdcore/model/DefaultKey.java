/*
 * Copyright 2016 Ville Koskela
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

import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;

/**
 * Default implementation of the {@link Key} interface.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@Loggable
public final class DefaultKey implements Key {

    @Override
    public ImmutableMap<String, String> getParameters() {
        return _dimensions;
    }

    @Override
    public String getCluster() {
        return _dimensions.get(CLUSTER_DIMENSION_KEY);
    }

    @Override
    public String getService() {
        return _dimensions.get(SERVICE_DIMENSION_KEY);
    }

    @Override
    public String getHost() {
        return _dimensions.get(HOST_DIMENSION_KEY);
    }

    @Override
    public Key filter(final ImmutableSet<String> keys) {
        return new DefaultKey(
                _dimensions.entrySet()
                        .stream()
                        .filter(e -> keys.contains(e.getKey()))
                        .collect(ImmutableMap.toImmutableMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue)));
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof DefaultKey)) {
            return false;
        }

        final DefaultKey otherKey = (DefaultKey) other;
        return Objects.equals(_dimensions, otherKey._dimensions);
    }

    @Override
    public int hashCode() {
        return _hashCode;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("Dimensions", _dimensions)
                .toString();
    }

    /**
     * Public constructor.
     *
     * @param dimensions The dimension key-value pairs.
     */
    public DefaultKey(final ImmutableMap<String, String> dimensions) {
        _dimensions = dimensions;
        _hashCode = _dimensions.hashCode();
    }

    private final int _hashCode;
    private final ImmutableMap<String, String> _dimensions;

    //private static final long serialVersionUID = -6065569917813181990L;
}
