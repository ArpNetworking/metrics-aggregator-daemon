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
package com.arpnetworking.metrics.mad.actors;

import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import scala.compat.java8.JFunction;

/**
 * Serves as the supervisory parent for all actor-based sources.
 * Will create an actor with {@link Props} obtained from StartSource
 * message as a child with the actorName in the message.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class SourceSupervisor extends UntypedActor {
    /**
     * Creates a {@link Props}.
     *
     * @return a {@link Props} to create an actor.
     */
    public static Props props() {
        return Props.create(SourceSupervisor.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReceive(final Object message) throws Exception {
        if (message instanceof StartSource) {
            final StartSource startSource = (StartSource) message;
            context().actorOf(startSource.getProps(), startSource.getName());
        } else if (message instanceof StopSource) {
            final StopSource stopSource = (StopSource) message;
            context().child(stopSource.getName())
                    .foreach(JFunction.proc(ref -> ref.tell(PoisonPill.getInstance(), self())));
        } else {
            unhandled(message);
        }
    }

    /**
     * Message to start a source actor.
     */
    public static class StartSource {
        public Props getProps() {
            return _props;
        }

        public String getName() {
            return _name;
        }

        /**
         * Public constructor.
         *
         * @param props {@link Props} to create the actor with.
         * @param name name of the actor to create.
         */
        public StartSource(final Props props, final String name) {
            _props = props;
            _name = name;
        }

        private final Props _props;
        private final String _name;
    }

    /**
     * Message to stop a source actor.
     */
    public static class StopSource {
        public String getName() {
            return _name;
        }

        /**
         * Public constructor.
         *
         * @param name Name of the actor to stop.
         */
        public StopSource(final String name) {
            _name = name;
        }

        private final String _name;
    }
}
