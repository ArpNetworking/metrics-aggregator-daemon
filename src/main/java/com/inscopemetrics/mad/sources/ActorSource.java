/*
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
package com.inscopemetrics.mad.sources;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.base.Suppliers;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Serves as a base class for actor-based sources.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public abstract class ActorSource extends BaseSource {
    @Override
    public void start() {
        if (_actor == null) {
            _actor = _actorSystem.actorOf(createProps(), getActorSafeName());
        }
    }

    @Override
    public void stop() {
        if (_actor != null) {
            _actor.tell(PoisonPill.getInstance(), ActorRef.noSender());
            _actor = null;
        }
    }

    /**
     * Return the actor safe name of this source.
     *
     * @return the actor safe name of this source
     */
    public String getActorSafeName() {
        return _actorSafeNameSupplier.get();
    }

    /**
     * Create a {@code Props} for the actor to be created at the provided path.
     *
     * @return a {@code Props} to create the actor with
     */
    protected abstract Props createProps();

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected ActorSource(final Builder<?, ? extends ActorSource> builder) {
        super(builder);
        if (builder._actorName != null) {
            _actorSafeNameSupplier = Suppliers.ofInstance(builder._actorName);
        } else {
            _actorSafeNameSupplier = Suppliers.memoize(() -> {
                final String safeName = getName().replaceAll("[^a-zA-Z0-9-_.*$+:@&=,!~';\\.]", "_");
                if (safeName.startsWith("$")) {
                    return "_" + safeName.substring(1);
                }
                return safeName;
            });
        }
        _actorSystem = builder._actorSystem;
    }

    private ActorRef _actor = null;

    private final Supplier<String> _actorSafeNameSupplier;
    private final ActorSystem _actorSystem;

    /**
     * ActorSource {@link BaseSource.Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public abstract static class Builder<B extends Builder<B, S>, S extends Source> extends BaseSource.Builder<B, S> {
        /**
         * Protected constructor.
         *
         * @param targetConstructor the constructor for the concrete type to be created by this builder
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        /**
         * Sets the actor path. Optional. Default is a actor safe version of
         * the source name. Cannot be null or empty.
         *
         * @param value the actor name
         * @return this instance of {@link Builder}
         */
        public final B setActorName(final String value) {
            _actorName = value;
            return self();
        }

        /**
         * Sets the actor system to launch the actor in. Required. Injectable.
         *
         * @param value the actor system
         * @return this instance of {@link Builder}
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
