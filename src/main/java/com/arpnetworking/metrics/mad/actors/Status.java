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
package com.arpnetworking.metrics.mad.actors;

import akka.actor.UntypedActor;

/**
 * Actor to determine the status of the system.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class Status extends UntypedActor {

    @Override
    public void onReceive(final Object message) throws Exception {
        if (IS_HEALTHY.equals(message)) {
            // TODO(vkoskela): Implement a deep health check [MAI-?]
            getSender().tell(Boolean.TRUE, self());
        }
    }

    /**
     * Message to request service health.
     */
    public static final String IS_HEALTHY = "isHealthy";
}
