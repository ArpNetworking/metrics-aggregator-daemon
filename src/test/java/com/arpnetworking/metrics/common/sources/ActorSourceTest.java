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

import akka.actor.ActorSystem;
import com.arpnetworking.metrics.mad.actors.SourceSupervisor;
import org.junit.After;
import org.junit.Before;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Test base for actor sources.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public abstract class ActorSourceTest {
    @Before
    public void setUpAkka() {
        _system = ActorSystem.create();
        // Launch root source actor
        _system.actorOf(SourceSupervisor.props(), "source");
    }

    @After
    public void tearDownAkka() throws Exception {
        Await.result(_system.terminate(), Duration.create(10, TimeUnit.SECONDS));
    }

    public ActorSystem getSystem() {
        return _system;
    }

    private ActorSystem _system;
}
