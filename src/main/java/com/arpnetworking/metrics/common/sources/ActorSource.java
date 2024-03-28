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
package com.arpnetworking.metrics.common.sources;

import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.annotation.JacksonInject;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.PoisonPill;
import org.apache.pekko.actor.Props;
import org.apache.pekko.pattern.Patterns;
import org.apache.pekko.routing.RoundRobinPool;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Serves as a base class for actor-based sources.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public abstract class ActorSource extends BaseSource {
    @Override
    public void start() {
        if (_actor == null) {
            _actor = _actorSystem.actorOf(new RoundRobinPool(_poolSize).props(createProps()), _actorName);
        }
    }

    @Override
    public void stop() {
        if (_actor != null) {
            try {
                Patterns.gracefulStop(
                        _actor,
                        SHUTDOWN_TIMEOUT,
                        PoisonPill.getInstance()).toCompletableFuture().get(
                                SHUTDOWN_TIMEOUT.toMillis(),
                                TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                LOGGER.warn()
                        .setMessage("Interrupted stopping actor source")
                        .addData("name", getName())
                        .addData("actor", _actor)
                        .addData("actorName", _actorName)
                        .log();
            } catch (final TimeoutException | ExecutionException e) {
                LOGGER.error()
                        .setMessage("Actor source stop timed out or failed")
                        .addData("name", getName())
                        .addData("actor", _actor)
                        .addData("actorName", _actorName)
                        .addData("timeout", SHUTDOWN_TIMEOUT)
                        .setThrowable(e)
                        .log();
            }
            _actor = null;
        }
    }

    /**
     * Return the {@link ActorSystem} used by this source.
     *
     * @return The {@link ActorSystem} used by this source.
     */
    protected ActorSystem getActorSystem() {
        return _actorSystem;
    }

    /**
     * Return an {@link ActorRef} to this source's Pekko actor.
     *
     * @return An {@link ActorRef} to this source's Pekko actor.
     */
    protected ActorRef getActor() {
        return _actor;
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
     * @param builder Instance of {@link Builder}.
     */
    protected ActorSource(final Builder<?, ? extends ActorSource> builder) {
        super(builder);
        _actorName = builder._actorName;
        _actorSystem = builder._actorSystem;
        _poolSize = builder._poolSize;
    }

    private ActorRef _actor = null;

    private final String _actorName;
    private final ActorSystem _actorSystem;
    private final int _poolSize;

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(1);
    private static final Logger LOGGER = LoggerFactory.getLogger(ActorSource.class);

    /**
     * ActorSource {@link BaseSource.Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
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

        /**
         * Sets the actor pool size.
         * @param value Number of actors in the pool
         * @return This instance of {@link Builder}
         */
        public final B setPoolSize(final Integer value) {
            _poolSize = value;
            return self();
        }

        @NotNull
        @NotEmpty
        private String _actorName;
        @NotNull
        @Min(1)
        private Integer _poolSize = 1;
        @JacksonInject
        private ActorSystem _actorSystem;
    }
}
