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

package com.arpnetworking.metrics.proxy.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.proxy.models.messages.LogFileAppeared;
import com.arpnetworking.metrics.proxy.models.messages.LogFileDisappeared;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Actor responsible for discovering files that are created or removed.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class LogScanner extends AbstractActor {

    /**
     * Public constructor.
     *
     * @param fileSourceManager <code>ActorRef</code> for the singleton FileSourceManager actor
     * @param logs The <code>List</code> of files to monitor.
     */
    @Inject
    public LogScanner(@Named("FileSourceManager") final ActorRef fileSourceManager, final List<Path> logs) {

        _fileSourceManagerActor = fileSourceManager;
        _logs = logs;

        LOGGER.debug()
                .setMessage("Created log scanner")
                .addData("actor", self())
                .addData("fileSourceManagerActor", _fileSourceManagerActor)
                .addData("logs", _logs)
                .log();

        _nonExistingLogs.addAll(_logs);

        context().system().scheduler().schedule(
                FiniteDuration.Zero(),
                FiniteDuration.apply(10, TimeUnit.SECONDS),
                self(),
                "tick",
                context().dispatcher(),
                self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("tick", message -> {
                    LOGGER.debug()
                            .setMessage("Searching for created/deleted logs")
                            .addData("actor", self())
                            .addData("scanner", this.toString())
                            .log();
                    for (final Path logFile : _logs) {
                        if (_nonExistingLogs.contains(logFile) && Files.exists(logFile)) {
                            LOGGER.info()
                                    .setMessage("Log file materialized")
                                    .addData("actor", self())
                                    .addData("file", logFile)
                                    .log();
                            _fileSourceManagerActor.tell(new LogFileAppeared(logFile), getSelf());
                            _nonExistingLogs.remove(logFile);
                            _existingLogs.add(logFile);
                        } else if (_existingLogs.contains(logFile) && Files.notExists(logFile)) {
                            LOGGER.info()
                                    .setMessage("Log file vanished")
                                    .addData("actor", self())
                                    .addData("file", logFile)
                                    .log();
                            _fileSourceManagerActor.tell(new LogFileDisappeared(logFile), getSelf());
                            _existingLogs.remove(logFile);
                            _nonExistingLogs.add(logFile);
                        }
                    }
                })
                .build();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("fileSourceManagerActor", _fileSourceManagerActor)
                .put("logs", _logs)
                .put("existingLogs", _existingLogs)
                .put("nonExistingLogs", _nonExistingLogs)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private final ActorRef _fileSourceManagerActor;
    private final List<Path> _logs;
    private final Set<Path> _existingLogs = Sets.newHashSet();
    private final Set<Path> _nonExistingLogs = Sets.newHashSet();

    private static final Logger LOGGER = LoggerFactory.getLogger(LogScanner.class);
}
