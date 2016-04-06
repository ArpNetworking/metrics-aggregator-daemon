/**
 * Copyright 2016 Smartsheet
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
package com.arpnetworking.metrics.common.sources;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.arpnetworking.metrics.mad.actors.SourceSupervisor;
import com.fasterxml.jackson.annotation.JacksonInject;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.util.function.Function;

/**
 * Serves as a base calss for actor-based sources.  Actor lifecycle is
 * handled by the {@link SourceSupervisor}.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public abstract class ActorSource extends BaseSource {
    public String getActorName() {
        return _actorName;
    }

    public ActorSystem getActorSystem() {
        return _actorSystem;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        _actorSystem.actorSelection("/user/source").tell(
                new SourceSupervisor.StartSource(createProps(), _actorName),
                ActorRef.noSender());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        _actorSystem.actorSelection("/user/source").tell(
                new SourceSupervisor.StopSource(_actorName),
                ActorRef.noSender());
    }

    /**
     * Create a props for the actor to be created at the provided path.
     *
     * @return A props to create the actor with.
     */
    protected abstract Props createProps();

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected ActorSource(final Builder<?, ? extends ActorSource> builder) {
        super(builder);
        _actorName = builder._actorName;
        _actorSystem = builder._actorSystem;
    }

    private final String _actorName;
    private final ActorSystem _actorSystem;

    /**
     * ActorSource {@link BaseSource.Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     * @author Ville Koskela (vkoskela at groupon dot com)
     */
    public abstract static class Builder<B extends Builder<B, S>, S extends Source> extends BaseSource.Builder<B, S> {
        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor The constructor for the concrete type to be created by this builder.
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        /**
         * Sets the actor path. Cannot be null or empty.
         *
         * @param value The name.
         * @return This instance of {@link Builder}
         */
        public final B setActorName(final String value) {
            _actorName = value;
            return self();
        }

        /**
         * Sets the actor system to launch the actor in.
         *
         * @param value The actor system.
         * @return This instance of {@link Builder}
         */
        public final B setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return self();
        }

        @NotNull
        @NotEmpty
        private String _actorName;
        @JacksonInject
        private ActorSystem _actorSystem;
    }
}
