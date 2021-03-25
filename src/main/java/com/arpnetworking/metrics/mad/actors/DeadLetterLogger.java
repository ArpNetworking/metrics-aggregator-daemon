/*
 * Copyright 2020 Dropbox
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

import akka.actor.AbstractActor;
import akka.actor.DeadLetter;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;

/**
 * Actor that logs dead letters.
 *
 * @author Ville Koskela (ville at inscopemetrics dot io)
 */
public class DeadLetterLogger  extends AbstractActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DeadLetter.class, this::log)
                .build();
    }

    private void log(final DeadLetter message) {
        LOGGER.info()
                .setMessage("Encountered dead letter")
                .addData("recipient", message.recipient())
                .addData("sender", message.sender())
                .addData("message", message.message())
                .log();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterLogger.class);
}
