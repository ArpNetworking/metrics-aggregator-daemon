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
package com.arpnetworking.configuration.jackson.module.pekko;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.LocalActorRef;
import org.apache.pekko.actor.TimerSchedulerImpl;

import java.io.Serial;

/**
 * Jackson module for serializing Pekko objects for use in JSON/Jackson based
 * logger serializers(e.g. logback-steno).
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class PekkoLoggingModule extends SimpleModule {

    /**
     * Public constructor.
     */
    public PekkoLoggingModule() { }

    @Override
    public void setupModule(final SetupContext context) {
        addSerializer(ActorRef.class, new ActorRefLoggingSerializer());
        addSerializer(LocalActorRef.class, new ActorRefLoggingSerializer());
        addSerializer(TimerSchedulerImpl.TimerMsg.class, new TimerMessageSerializer());
        addSerializer(TimerSchedulerImpl.InfluenceReceiveTimeoutTimerMsg.class, new TimerMessageSerializer());
        addSerializer(TimerSchedulerImpl.NotInfluenceReceiveTimeoutTimerMsg.class, new TimerMessageSerializer());
        super.setupModule(context);
    }

    @Serial
    private static final long serialVersionUID = 6984539942087839964L;
}
