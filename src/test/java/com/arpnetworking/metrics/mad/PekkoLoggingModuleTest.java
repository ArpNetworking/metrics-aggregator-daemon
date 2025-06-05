/*
 * Copyright 2025 Brandon Arp
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
package com.arpnetworking.metrics.mad;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.joran.spi.JoranException;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.TestLoggerFactory;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Terminated;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Unit test class for verifying the functionality and integration of the AkkaLoggingModule.
 * This class tests various aspects of logging and actor-based communication within an Akka-based
 * system using Logback as the logging backend.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class PekkoLoggingModuleTest {
    /**
     * Sets up the test environment by initializing the required components.
     * This method is executed before each test, ensuring a fresh setup.
     *
     * The setup process includes the following:
     * - Creation of an Akka {@link ActorSystem} instance.
     * - Loading the specific XML configuration file corresponding to the test class.
     * - Initializing and configuring a Logback {@link LoggerContext}.
     * - Setting up the {@link JoranConfigurator} to parse the configuration file.
     * - Resetting and associating a {@link LogbackMDCAdapter} instance with the LoggerContext.
     *
     * If an error occurs during the configuration process using the JoranConfigurator,
     * a {@link RuntimeException} is thrown, encapsulating the underlying {@link JoranException}.
     *
     * The initialized {@link ActorSystem} and {@link LoggerContext} serve as the foundation for
     * executing tests that involve logging and actor-based messaging.
     */
    @Before
    public void setUp() {
        _actorSystem = ActorSystem.create();
        final URL configuration = getClass().getResource(
                this.getClass().getSimpleName() + ".xml");
        _loggerContext = new LoggerContext();
        final JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(_loggerContext);
        _loggerContext.reset();
        final LogbackMDCAdapter mdcAdapter = new LogbackMDCAdapter();
        _loggerContext.setMDCAdapter(mdcAdapter);
        try {
            configurator.doConfigure(configuration);
        } catch (final JoranException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        final CompletionStage<Terminated> terminated = _actorSystem.getWhenTerminated();
        _actorSystem.terminate();
        terminated.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testLoggingActorRefs() {
        final ActorRef actorRef = _actorSystem.actorOf(Props.create(SimpleActor.class, SimpleActor::new), "simple-actor");
        getLogger().info()
                .setMessage("LoggingActorRefs")
                .addData("actorRef", actorRef)
                .log();
        assertOutput();
    }

    protected void assertOutput() {
        final URL expectedResource = getClass().getResource(
                getClass().getSimpleName() + ".expected");
        final File actualFile = new File("target/test-logs/" + this.getClass().getSimpleName() + ".log");
        final String actualOutput;
        try {
            actualOutput = Files.readString(actualFile.toPath());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        try {
            assertOutput(Files.readString(Paths.get(expectedResource.toURI())), actualOutput);
        } catch (final IOException | URISyntaxException e) {
            Assert.fail("Failed with exception: " + e);
        }
    }

    protected void assertOutput(final String expected, final String actual) {
        Assert.assertEquals(expected.trim(), sanitizeOutput(actual.trim()));
    }

    protected String sanitizeOutput(final String output) {
        return output.replaceAll("\"time\":\"[^\"]+\"", "\"time\":\"<TIME>\"")
                .replaceAll("\"id\":\"[^\"]+\"", "\"id\":\"<ID>\"")
                .replaceAll("Actor\\[pekko://default/user/simple-actor[^\"]+]\"", "Actor[pekko://default/user/simple-actor]\"")
                .replaceAll("\"host\":\"[^\"]+\"", "\"host\":\"<HOST>\"")
                .replaceAll("\"processId\":\"[^\"]+\"", "\"processId\":\"<PROCESS_ID>\"")
                .replaceAll("\"threadId\":\"[^\"]+\"", "\"threadId\":\"<THREAD_ID>\"")
                .replaceAll("\"backtrace\":\\[[^\\]]+\\]", "\"backtrace\":[]")
                .replaceAll("\"_id\":\"[^\"]+\"", "\"_id\":\"<ID>\"");
    }

    protected Logger getLogger() {
        return TestLoggerFactory.getLogger(_loggerContext.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME));
    }

    private ActorSystem _actorSystem;
    private LoggerContext _loggerContext;

    static class SimpleActor extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }
    }
}
