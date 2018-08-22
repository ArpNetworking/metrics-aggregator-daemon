/*
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
package com.inscopemetrics.mad.sinks;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.steno.LogValueMapFactory;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.ImmutableSet;
import com.inscopemetrics.mad.model.PeriodicData;
import com.inscopemetrics.mad.statistics.Statistic;
import com.inscopemetrics.mad.telemetry.actors.TelemetryActor;
import net.sf.oval.constraint.NotNull;

/**
 * A publisher that sends a message to the {@link TelemetryActor} actor.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TelemetrySink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        _telemetryActor.tell(periodicData, ActorRef.noSender());
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("telemetryActor", _telemetryActor)
                .build();
    }

    private TelemetrySink(final Builder builder) {
        super(builder);

        // Create the telemetry actor
        _telemetryActor = builder._actorSystem.actorOf(
                TelemetryActor.props(builder._metricsFactory, builder._histogramStatistics),
                TELEMETRY_ACTOR_NAME);
    }

    private final ActorRef _telemetryActor;

    /**
     * The path to the {@link TelemetryActor} instance.
     */
    public static final String TELEMETRY_ACTOR_NAME = "telemetry";

    /**
     * TelemetrySink {@code Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, TelemetrySink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TelemetrySink::new);
        }

        /**
         * Sets the actor system to create the sink actor in. Required. Cannot
         * be null. Injected by default.
         *
         * @param value the actor system
         * @return this builder
         */
        public Builder setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return self();
        }

        /**
         * Sets the metrics factory. Required. Cannot be null. Injected by default.
         *
         * @param value the metrics factory
         * @return this builder
         */
        public Builder setMetricsFactory(final MetricsFactory value) {
            _metricsFactory = value;
            return self();
        }

        /**
         * Sets the set of statistics to send in lieu of a histogram. Optional.
         * Cannot be null. Empty set by default.
         *
         * @param value the set of statistics to send in lieu of histogram
         * @return this builder
         */
        public Builder setHistogramStatistics(final ImmutableSet<Statistic> value) {
            _histogramStatistics = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @JacksonInject
        @NotNull
        private ActorSystem _actorSystem;
        @JacksonInject
        @NotNull
        private MetricsFactory _metricsFactory;
        @NotNull
        private ImmutableSet<Statistic> _histogramStatistics = ImmutableSet.of();
    }
}
