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

import com.arpnetworking.configuration.Listener;
import com.arpnetworking.configuration.Trigger;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.utility.Launchable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamic configuration implementation of <code>Configuration</code>.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class DynamicConfiguration extends BaseJacksonConfiguration implements Launchable {

    /**
     * {@inheritDoc}
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("snapshot", _snapshot)
                .put("sourceBuilders", _sourceBuilders)
                .put("listeners", _listeners)
                .put("triggerEvaluator", _triggerEvaluator)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JsonNodeSource getJsonSource() {
        return _snapshot.get().getJsonSource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void launch() {
        LOGGER.debug()
                .setMessage("Launching")
                .addData("component", this)
                .log();
        _triggerEvaluatorExecutor = Executors.newSingleThreadExecutor((runnable) -> new Thread(runnable, "DynamicConfigTriggerEvaluator"));
        _triggerEvaluatorExecutor.execute(_triggerEvaluator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdown() {
        LOGGER.debug()
                .setMessage("Stopping")
                .addData("component", this)
                .log();
        try {
            _triggerEvaluator.stop();
            // CHECKSTYLE.OFF: IllegalCatch - Prevent dynamic configuration from shutting down.
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            LOGGER.error()
                    .setMessage("Stop failed")
                    .addData("component", this)
                    .addData("reason", "trigger evaluator failed to stop")
                    .setThrowable(e)
                    .log();
        }
        _triggerEvaluatorExecutor.shutdown();
        try {
            _triggerEvaluatorExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            LOGGER.warn()
                    .setMessage("Stop failed")
                    .addData("component", this)
                    .addData("reason", "trigger evaluator executor failed to stop")
                    .setThrowable(e)
                    .log();
        }
    }

    private void loadConfiguration() {
        final List<JsonNodeSource> sources =
                Lists.<com.arpnetworking.commons.builder.Builder<? extends JsonNodeSource>, JsonNodeSource>transform(
                        _sourceBuilders,
                        com.arpnetworking.commons.builder.Builder::build);

        final StaticConfiguration snapshot = new StaticConfiguration.Builder()
                .setObjectMapper(_objectMapper)
                .setSources(sources)
                .build();

        for (final Listener listener : _listeners) {
            try {
                LOGGER.debug()
                        .setMessage("Offering configuration")
                        .addData("listener", listener)
                        .log();
                listener.offerConfiguration(snapshot);
                // CHECKSTYLE.OFF: IllegalCatch - Any exception is considered validation failure.
            } catch (final Exception e) {
                // CHECKSTYLE.ON: IllegalCatch
                LOGGER.error()
                        .setMessage("Validation of offered configuration failed")
                        .addData("listener", listener)
                        .addData("configuration", snapshot)
                        .setThrowable(e)
                        .log();

                // TODO(vkoskela): Persist "good" configuration across restarts [MAI-?]
                // The code will leave the good configuration in the running instance
                // but the configuration sources may be in a state such that the next
                // restart will only have the latest (currently bad) configuration
                // available.

                return;
            }
        }

        _snapshot.set(snapshot);

        for (final Listener listener : _listeners) {
            try {
                LOGGER.debug()
                        .setMessage("Applying configuration")
                        .addData("listener", listener)
                        .log();
                listener.applyConfiguration();
                // CHECKSTYLE.OFF: IllegalCatch - Apply configuration to all instances.
            } catch (final Exception e) {
                // CHECKSTYLE.ON: IllegalCatch
                LOGGER.warn()
                        .setMessage("Application of new configuration failed")
                        .addData("listener", listener)
                        .addData("configuration", _snapshot)
                        .setThrowable(e)
                        .log();
            }
        }
    }

    private DynamicConfiguration(final Builder builder) {
        super(builder);
        _sourceBuilders = ImmutableList.copyOf(builder._sourceBuilders);
        _listeners = ImmutableList.copyOf(builder._listeners);

        _triggerEvaluator = new TriggerEvaluator(Lists.newArrayList(builder._triggers));
    }

    private final AtomicReference<StaticConfiguration> _snapshot = new AtomicReference<>();
    private final List<com.arpnetworking.commons.builder.Builder<? extends JsonNodeSource>> _sourceBuilders;
    private final List<Listener> _listeners;
    private final TriggerEvaluator _triggerEvaluator;

    private ExecutorService _triggerEvaluatorExecutor;

    private static final Duration TRIGGER_EVALUATION_INTERVAL = Duration.standardSeconds(60);
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConfiguration.class);

    private final class TriggerEvaluator implements Runnable {

        private TriggerEvaluator(final List<Trigger> triggers) {
            _triggers = triggers;
            _isRunning = true;
        }

        public void stop() {
            _isRunning = false;
        }

        @Override
        public void run() {
            Thread.currentThread().setUncaughtExceptionHandler(
                    (thread, throwable) -> LOGGER.error()
                            .setMessage("Unhandled exception")
                            .setThrowable(throwable)
                            .log());

            while (_isRunning) {
                // Evaluate all the triggers to ensure all triggers are reset
                // before loading the configuration.
                boolean reload = false;
                for (final Trigger trigger : _triggers) {
                    try {
                        reload = reload || trigger.evaluateAndReset();
                        // CHECKSTYLE.OFF: IllegalCatch - Evaluate and reset all triggers
                    } catch (final Throwable t) {
                        // CHECKSTYLE.ON: IllegalCatch
                        LOGGER.warn()
                                .setMessage("Failed to evaluate and reset trigger")
                                .addData("trigger", trigger)
                                .setThrowable(t)
                                .log();
                    }
                }

                // Reload the configuration
                if (reload) {
                    try {
                        loadConfiguration();
                        // CHECKSTYLE.OFF: IllegalCatch - Prevent thread from being killed
                    } catch (final Exception e) {
                        // CHECKSTYLE.ON: IllegalCatch
                        LOGGER.error()
                                .setMessage("Failed to load configuration")
                                .setThrowable(e)
                                .log();
                    }
                }

                // Wait for the next evaluation period
                try {
                    final DateTime sleepTimeout = DateTime.now().plus(TRIGGER_EVALUATION_INTERVAL);
                    while (DateTime.now().isBefore(sleepTimeout) && _isRunning) {
                        Thread.sleep(100);
                    }
                } catch (final InterruptedException e) {
                    LOGGER.debug()
                            .setMessage("Interrupted")
                            .addData("isRunning", _isRunning)
                            .setThrowable(e)
                            .log();
                }
            }
        }

        @LogValue
        public Object toLogValue() {
            return LogValueMapFactory.builder(this)
                    .put("isRunning", _isRunning)
                    .put("triggers", _triggers)
                    .build();
        }

        @Override
        public String toString() {
            return toLogValue().toString();
        }

        private final List<Trigger> _triggers;
        private volatile boolean _isRunning;
    }

    /**
     * Builder for <code>DynamicConfiguration</code>.
     */
    public static final class Builder extends BaseJacksonConfiguration.Builder<Builder, DynamicConfiguration> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DynamicConfiguration::new);
        }

        /**
         * Set the <code>List</code> of <code>JsonSource</code> instance
         * <code>Builder</code> instances. Cannot be null.
         *
         * @param value The <code>List</code> of <code>JsonSource</code>
         * instance <code>Builder</code> instances.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSourceBuilders(final List<com.arpnetworking.commons.builder.Builder<? extends JsonNodeSource>> value) {
            _sourceBuilders = Lists.newArrayList(value);
            return self();
        }

        /**
         * Add a <code>JsonSource</code> <code>Builder</code> instance.
         *
         * @param value The <code>JsonSource</code> <code>Builder</code> instance.
         * @return This <code>Builder</code> instance.
         */
        public Builder addSourceBuilder(final com.arpnetworking.commons.builder.Builder<? extends JsonNodeSource> value) {
            if (_sourceBuilders == null) {
                _sourceBuilders = Lists.newArrayList();
            }
            _sourceBuilders.add(value);
            return self();
        }

        /**
         * Set the <code>List</code> of <code>Trigger</code> instances. Cannot
         * be null.
         *
         * @param value The <code>List</code> of <code>Trigger</code> instances.
         * @return This <code>Builder</code> instance.
         */
        public Builder setTriggers(final List<Trigger> value) {
            _triggers = Lists.newArrayList(value);
            return self();
        }

        /**
         * Add a <code>ConfigurationTrigger</code> instance.
         *
         * @param value The <code>ConfigurationTrigger</code> instance.
         * @return This <code>Builder</code> instance.
         */
        public Builder addTrigger(final Trigger value) {
            if (_triggers == null) {
                _triggers = Lists.newArrayList();
            }
            _triggers.add(value);
            return self();
        }

        /**
         * Set the <code>List</code> of <code>Listener</code> instances. Cannot
         * be null.
         *
         * @param value The <code>List</code> of <code>Listener</code> instances.
         * @return This <code>Builder</code> instance.
         */
        public Builder setListeners(final List<Listener> value) {
            _listeners = Lists.newArrayList(value);
            return self();
        }

        /**
         * Add a <code>ConfigurationListener</code> instance.
         *
         * @param value The <code>ConfigurationListener</code> instance.
         * @return This <code>Builder</code> instance.
         */
        public Builder addListener(final Listener value) {
            if (_listeners == null) {
                _listeners = Lists.newArrayList();
            }
            _listeners.add(value);
            return self();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private List<com.arpnetworking.commons.builder.Builder<? extends JsonNodeSource>> _sourceBuilders;
        @NotNull
        private List<Trigger> _triggers = Lists.newArrayList();
        @NotNull
        private List<Listener> _listeners;
    }
}
