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

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.common.sources.FileSource;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.common.tailer.InitialPosition;
import com.arpnetworking.metrics.proxy.models.messages.LogFileAppeared;
import com.arpnetworking.metrics.proxy.models.messages.LogFileDisappeared;
import com.arpnetworking.metrics.proxy.models.messages.LogLine;
import com.arpnetworking.metrics.proxy.parsers.LogLineParser;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * Actor for handling messages related to live log reporting.
 *
 * @author Mohammed Kamel (mkamel at groupon dot com)
 */
public final class FileSourcesManager extends AbstractActor {
    //TODO(barp): Add metrics to this class [MAI-406]

    /**
     * Public constructor.
     *
     * @param streamContextActor {@link ActorRef} for the singleton stream context actor
     */
    @Inject
    public FileSourcesManager(@Named("StreamContext") final ActorRef streamContextActor) {
        _fileSourceInterval = Duration.ofMillis(500L);
        _streamContextActor = streamContextActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LogFileAppeared.class, message -> {
                    addSource(message.getFile());
                })
                .match(LogFileDisappeared.class, message -> {
                    removeSource(message.getFile());
                })
                .matchAny(message -> {
                    LOGGER.warn()
                            .setMessage("Unsupported message")
                            .addData("actor", self())
                            .addData("data", message)
                            .log();
                    unhandled(message);
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
                .put("fileSourceInterval", _fileSourceInterval)
                .put("fileSources", _fileSources)
                .put("streamContextActor", _streamContextActor)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private void addSource(final Path filepath) {
        LOGGER.info()
                .setMessage("Adding new log file source")
                .addData("actor", self())
                .addData("path", filepath)
                .log();
        if (!_fileSources.containsKey(filepath)) {
            final Source source =
                    new FileSource.Builder<LogLine>()
                            .setName("File: " + filepath)
                            .setSourceFile(filepath)
                            .setInterval(_fileSourceInterval)
                            .setParser(new LogLineParser(filepath))
                            .setInitialPosition(InitialPosition.END)
                            .build();
            source.attach(new LogFileObserver(_streamContextActor, getSelf()));
            source.start();

            _fileSources.put(filepath, source);

            _streamContextActor.tell(new LogFileAppeared(filepath), getSelf());
        }
    }

    private void removeSource(final Path filepath) {
        _streamContextActor.tell(new LogFileDisappeared(filepath), getSelf());
        LOGGER.info()
            .setMessage("Removing log file source")
            .addData("actor", self())
            .addData("path", filepath)
            .log();
        final Source source = _fileSources.remove(filepath);
        if (source != null) {
            source.stop();
        } else {
            LOGGER.warn()
                .setMessage("Attempted to removed a non existing file source")
                .addData("actor", self())
                .addData("path", filepath)
                .log();
        }
    }

    private final Duration _fileSourceInterval;
    private final Map<Path, Source> _fileSources = Maps.newHashMap();
    private final ActorRef _streamContextActor;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourcesManager.class);

    /* package private */ static final class LogFileObserver implements Observer {

        /* package private */ LogFileObserver(final ActorRef streamContextActor, final ActorRef messageSender) {
            _streamContextActor = streamContextActor;
            _messageSender = messageSender;
        }

        @Override
        public void notify(final Observable observable, final Object event) {
            final LogLine logLine = (LogLine) event;
            _streamContextActor.tell(logLine, _messageSender);
        }

        /**
         * Generate a Steno log compatible representation.
         *
         * @return Steno log compatible representation.
         */
        @LogValue
        public Object toLogValue() {
            return LogValueMapFactory.builder(this)
                    .put("streamContextActor", _streamContextActor)
                    .put("messageSender", _messageSender)
                    .build();
        }

        @Override
        public String toString() {
            return toLogValue().toString();
        }

        private final ActorRef _streamContextActor;
        private final ActorRef _messageSender;
    }
}
