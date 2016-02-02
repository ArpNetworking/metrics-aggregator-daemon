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
package com.arpnetworking.utility;

import com.arpnetworking.configuration.Configuration;
import com.arpnetworking.configuration.Listener;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.base.Optional;

/**
 * Manages configuration and reconfiguration of a <code>Relaunchable</code> instance
 * using a POJO representation of its configuration. The <code>Relaunchable</code>
 * is updated with each new configuration. The configuration must validate
 * on construction and throw an exception if the configuration is invalid.
 *
 * @param <T> The <code>Relaunchable</code> type to configure.
 * @param <S> The type representing the validated configuration.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class Reconfigurator<T extends Relaunchable<? super S>, S> implements Listener {

    /**
     * Public constructor.
     *
     * @param relaunchable The <code>Relaunchable</code> instance.
     * @param configurationClass The configuration class.
     */
    public Reconfigurator(final Relaunchable<S> relaunchable, final Class<? extends S> configurationClass) {
        _relaunchable = relaunchable;
        _configurationClass = configurationClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void offerConfiguration(final Configuration configuration) throws Exception {
        _offeredConfiguration = configuration.getAs(_configurationClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void applyConfiguration() {
        // Swap configurations
        _configuration = _offeredConfiguration;
        _relaunchable.relaunch(_configuration.get());
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("configurationClass", _configurationClass)
                .put("relaunchable", _relaunchable)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /* package private */ synchronized Relaunchable<S> getRelaunchable() {
        return _relaunchable;
    }

    /* package private */ synchronized Optional<S> getConfiguration() {
        return _configuration;
    }

    /* package private */ synchronized Optional<S> getOfferedConfiguration() {
        return _offeredConfiguration;
    }

    private final Relaunchable<S> _relaunchable;
    private final Class<? extends S> _configurationClass;

    private Optional<S> _configuration = Optional.absent();
    private Optional<S> _offeredConfiguration = Optional.absent();
}
