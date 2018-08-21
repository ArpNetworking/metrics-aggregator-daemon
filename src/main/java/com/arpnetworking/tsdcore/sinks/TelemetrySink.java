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
package com.arpnetworking.tsdcore.sinks;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.annotation.JacksonInject;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

/**
 * A publisher that sends a message to the <code>Telemetry</code> actor.
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
        return LogValueMapFactory.builder()
                .put("super", super.toLogValue())
                .put("telemetryActor", _telemetryActor)
                .build();
    }

    private TelemetrySink(final Builder builder) {
        super(builder);
        _telemetryActor = builder._actorSystem.actorSelection(builder._telemetryActorPath);
    }

    private final ActorSelection _telemetryActor;

    /**
     * Base <code>Builder</code> implementation.
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
         * Sets the actor system to create the sink actor in. Required. Cannot be null. Injected by default.
         *
         * @param value the actor system
         * @return this builder
         */
        public Builder setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return self();
        }

        /**
         * Sets the <code>Telemetry</code> actor path. Optional. Cannot be null or empty. "/users/telemetry" by default.
         *
         * @param value the path to the <code>Telemetry</code> actor
         * @return this builder
         */
        public Builder setTelemetryActorPath(final String value) {
            _telemetryActorPath = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @JacksonInject
        @NotNull
        private ActorSystem _actorSystem;
        @NotNull
        @NotEmpty
        private String _telemetryActorPath = "/user/telemetry";
    }
}
