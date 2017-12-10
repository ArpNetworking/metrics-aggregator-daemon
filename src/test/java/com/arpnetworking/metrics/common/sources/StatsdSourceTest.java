/**
 * Copyright 2017 Inscope Metrics, Inc
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
import akka.testkit.javadsl.TestKit;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.tsdcore.model.MetricType;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.collect.ImmutableMap;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import scala.concurrent.duration.Duration;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the StatsdSource class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class StatsdSourceTest {

    private ActorSystem _actorSystem;

    @Captor
    private ArgumentCaptor<Record> _recordCaptor;

    @Before
    public void setUp() {
        _actorSystem =  ActorSystem.create();
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(_actorSystem);
    }

    @Test
    public void test() {
        final StatsdSource statsdSource = new StatsdSource.Builder()
                .setActorSystem(_actorSystem)
                .setActorName("StatsdSourceTest.testActor")
                .setName("StatsdSourceTest.test")
                .setPort(1234)
                .build();
        final Observer observer = Mockito.mock(Observer.class);
        statsdSource.attach(observer);

        // CHECKSTYLE.OFF: AnonInnerLength - This is the Akka test pattern
        new TestKit(_actorSystem) {{
            // Deploy the statsd source actor
            final ActorRef statsdSourceActor = _actorSystem.actorOf(StatsdSource.Actor.props(statsdSource));

            // Wait for it to be ready
            boolean isReady = false;
            while (!isReady) {
                statsdSourceActor.tell("IsReady", getRef());
                isReady = expectMsgClass(Duration.create(10, TimeUnit.SECONDS), Boolean.class);
            }

            // Send metrics using statsd over udp
            final StatsDClient statsdClient = new NonBlockingStatsDClient("StatsdSourceTest.test", "localhost", 1234);
            statsdClient.count("counter1", 3);
            statsdClient.stop();

            // Captor observation of the resulting records
            Mockito.verify(observer, Mockito.timeout(1000)).notify(
                    Mockito.same(statsdSource),
                    _recordCaptor.capture());

            // Verify the captured records
            assertRecordEquality(
                    new DefaultRecord.Builder()
                            .setTime(DateTime.now())
                            .setId(UUID.randomUUID().toString())
                            .setMetrics(ImmutableMap.of(
                                    "StatsdSourceTest.test.counter1",
                                    new DefaultMetric.Builder()
                                            .setType(MetricType.COUNTER)
                                            .setValues(Collections.singletonList(
                                                    new Quantity.Builder()
                                                            .setValue(3d)
                                                            .build()))
                                            .build()))
                            .build(),
                    _recordCaptor.getValue());
        }};
        // CHECKSTYLE.ON: AnonInnerLength
    }

    private void assertRecordEquality(final Record expected, final Record actual) {
        Assert.assertEquals(expected.getAnnotations(), actual.getAnnotations());
        Assert.assertEquals(expected.getDimensions(), actual.getDimensions());
        Assert.assertEquals(expected.getMetrics(), actual.getMetrics());
    }
}
