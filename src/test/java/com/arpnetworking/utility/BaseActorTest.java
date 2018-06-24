/*
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
package com.arpnetworking.utility;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.After;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Base for actor tests. Loads configuration and provides an actor system.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
// TODO(barp): Pull this into a test-utils package [MAI-488]
public abstract class BaseActorTest {
    /**
     * Binds mockito annotations and starts the actor system.
     */
    @Before
    public void startup() {
        MockitoAnnotations.initMocks(this);
        _system = ActorSystem.create();
    }

    /**
     * Shuts down the actor system.
     */
    @After
    public void shutdown() throws Exception {
        final Future<Terminated> terminate = _system.terminate();
        Await.result(terminate, Duration.apply(30, TimeUnit.SECONDS));
    }

    protected ActorSystem getSystem() {
        return _system;
    }

    private ActorSystem _system;
}
