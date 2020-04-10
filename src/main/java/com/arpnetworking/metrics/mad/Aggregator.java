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
package com.arpnetworking.metrics.mad;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.arpnetworking.utility.Launchable;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Performs aggregation of <code>Record</code> instances per <code>Period</code>.
 * This class is thread safe.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public final class Aggregator implements Observer, Launchable {

    @Override
    public synchronized void launch() {
        LOGGER.debug()
                .setMessage("Launching aggregator")
                .addData("aggregator", this)
                .log();
    }

    @Override
    public synchronized void shutdown() {
        LOGGER.debug()
                .setMessage("Stopping aggregator")
                .addData("aggregator", this)
                .log();

        for (final List<ActorRef> actorRefList : _periodWorkerActors.values()) {
            for (final ActorRef actorRef : actorRefList) {
                actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
            }
        }
    }

    @Override
    public void notify(final Observable observable, final Object event) {
        if (!(event instanceof Record)) {
            LOGGER.error()
                    .setMessage("Observed unsupported event")
                    .addData("event", event)
                    .log();
            return;
        }

        final Record record = (Record) event;
        final Key key = new DefaultKey(record.getDimensions());
        LOGGER.trace()
                .setMessage("Sending record to aggregation actor")
                .addData("record", record)
                .addData("key", key)
                .log();

        for (final ActorRef periodWorkerActor : _periodWorkerActors.computeIfAbsent(key, this::createActors)) {
            periodWorkerActor.tell(record, ActorRef.noSender());
        }
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("sink", _sink)
                .put("timerStatistics", _specifiedTimerStatistics)
                .put("counterStatistics", _specifiedCounterStatistics)
                .put("gaugeStatistics", _specifiedGaugeStatistics)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private List<ActorRef> createActors(final Key key) {
        final List<ActorRef> periodWorkerList = Lists.newArrayListWithExpectedSize(_periods.size());
        for (final Duration period : _periods) {
            final Bucket.Builder bucketBuilder = new Bucket.Builder()
                    .setKey(key)
                    .setSpecifiedCounterStatistics(_specifiedCounterStatistics)
                    .setSpecifiedGaugeStatistics(_specifiedGaugeStatistics)
                    .setSpecifiedTimerStatistics(_specifiedTimerStatistics)
                    .setDependentCounterStatistics(_dependentCounterStatistics)
                    .setDependentGaugeStatistics(_dependentGaugeStatistics)
                    .setDependentTimerStatistics(_dependentTimerStatistics)
                    .setSpecifiedStatistics(_cachedSpecifiedStatistics)
                    .setDependentStatistics(_cachedDependentStatistics)
                    .setPeriod(period)
                    .setSink(_sink);
            periodWorkerList.add(_actorSystem.actorOf(Props.create(PeriodWorker.class, period, bucketBuilder)));
        }
        LOGGER.debug()
                .setMessage("Created period worker actors")
                .addData("key", key)
                .addData("periodWorkersSize", periodWorkerList.size())
                .log();
        return periodWorkerList;
    }

    private ImmutableSet<Statistic> computeDependentStatistics(final ImmutableSet<Statistic> statistics) {
        final ImmutableSet.Builder<Statistic> builder = ImmutableSet.builder();
        for (final Statistic statistic : statistics) {
            statistic.getDependencies().stream().filter(dependency -> !statistics.contains(dependency)).forEach(builder::add);
        }
        return builder.build();
    }

    private Aggregator(final Builder builder) {
        _actorSystem = builder._actorSystem;
        _periods = ImmutableSet.copyOf(builder._periods);
        _sink = builder._sink;
        _specifiedCounterStatistics = ImmutableSet.copyOf(builder._counterStatistics);
        _specifiedGaugeStatistics = ImmutableSet.copyOf(builder._gaugeStatistics);
        _specifiedTimerStatistics = ImmutableSet.copyOf(builder._timerStatistics);
        _dependentCounterStatistics = computeDependentStatistics(_specifiedCounterStatistics);
        _dependentGaugeStatistics = computeDependentStatistics(_specifiedGaugeStatistics);
        _dependentTimerStatistics = computeDependentStatistics(_specifiedTimerStatistics);
        final ImmutableMap.Builder<Pattern, ImmutableSet<Statistic>> statisticsBuilder = ImmutableMap.builder();
        for (final Map.Entry<String, Set<Statistic>> entry : builder._statistics.entrySet()) {
            final Pattern pattern = Pattern.compile(entry.getKey());
            final ImmutableSet<Statistic> statistics = ImmutableSet.copyOf(entry.getValue());
            statisticsBuilder.put(pattern, statistics);
        }
        _statistics = statisticsBuilder.build();

        _cachedSpecifiedStatistics = CacheBuilder
                .newBuilder()
                .concurrencyLevel(1)
                .build(
                        new CacheLoader<String, Optional<ImmutableSet<Statistic>>>() {
                            // TODO(vkoskela): Add @NonNull annotation to metric. [ISSUE-?]
                            @Override
                            public Optional<ImmutableSet<Statistic>> load(final String metric) throws Exception {
                                for (final Map.Entry<Pattern, ImmutableSet<Statistic>> entry : _statistics.entrySet()) {
                                    final Pattern pattern = entry.getKey();
                                    final ImmutableSet<Statistic> statistics = entry.getValue();
                                    if (pattern.matcher(metric).matches()) {
                                        return Optional.of(statistics);
                                    }
                                }
                                return Optional.empty();
                            }
                        });
        _cachedDependentStatistics = CacheBuilder
                .newBuilder()
                .concurrencyLevel(1)
                .build(new CacheLoader<String, Optional<ImmutableSet<Statistic>>>() {
                            // TODO(vkoskela): Add @NonNull annotation to metric. [ISSUE-?]
                            @Override
                            public Optional<ImmutableSet<Statistic>> load(final String metric) throws Exception {
                                final Optional<ImmutableSet<Statistic>> statistics = _cachedSpecifiedStatistics.get(metric);
                                return statistics.map(statisticImmutableSet -> computeDependentStatistics(statisticImmutableSet));
                           }
                        });
    }

    private final ActorSystem _actorSystem;
    private final ImmutableSet<Duration> _periods;
    private final Sink _sink;
    private final ImmutableSet<Statistic> _specifiedTimerStatistics;
    private final ImmutableSet<Statistic> _specifiedCounterStatistics;
    private final ImmutableSet<Statistic> _specifiedGaugeStatistics;
    private final ImmutableSet<Statistic> _dependentTimerStatistics;
    private final ImmutableSet<Statistic> _dependentCounterStatistics;
    private final ImmutableSet<Statistic> _dependentGaugeStatistics;
    private final ImmutableMap<Pattern, ImmutableSet<Statistic>> _statistics;
    private final LoadingCache<String, Optional<ImmutableSet<Statistic>>> _cachedSpecifiedStatistics;
    private final LoadingCache<String, Optional<ImmutableSet<Statistic>>> _cachedDependentStatistics;
    private final Map<Key, List<ActorRef>> _periodWorkerActors = Maps.newConcurrentMap();

    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregator.class);

    /**
     * <code>Builder</code> implementation for <code>Aggregator</code>.
     */
    public static final class Builder extends OvalBuilder<Aggregator> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(Aggregator::new);
        }

        /**
         * Set the Akka {@link ActorSystem}. Cannot be null.
         *
         * @param value The Akka {@link ActorSystem}.
         * @return This <code>Builder</code> instance.
         */
        public Builder setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return this;
        }

        /**
         * Set the sink. Cannot be null or empty.
         *
         * @param value The sink.
         * @return This <code>Builder</code> instance.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * Set the periods. Cannot be null or empty.
         *
         * @param value The periods.
         * @return This <code>Builder</code> instance.
         */
        public Builder setPeriods(final Set<Duration> value) {
            _periods = value;
            return this;
        }

        /**
         * Set the timer statistics. Cannot be null or empty.
         *
         * @param value The timer statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setTimerStatistics(final Set<Statistic> value) {
            _timerStatistics = value;
            return this;
        }

        /**
         * Set the counter statistics. Cannot be null or empty.
         *
         * @param value The counter statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setCounterStatistics(final Set<Statistic> value) {
            _counterStatistics = value;
            return this;
        }

        /**
         * Set the gauge statistics. Cannot be null or empty.
         *
         * @param value The gauge statistics.
         * @return This <code>Builder</code> instance.
         */
        public Builder setGaugeStatistics(final Set<Statistic> value) {
            _gaugeStatistics = value;
            return this;
        }

        /**
         * The statistics to compute for a metric pattern. Optional. Cannot be null.
         * Default is empty.
         *
         * @param value The gauge statistics.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setStatistics(final Map<String, Set<Statistic>> value) {
            _statistics = value;
            return this;
        }

        @NotNull
        private ActorSystem _actorSystem;
        @NotNull
        private Sink _sink;
        @NotNull
        private Set<Duration> _periods;
        @NotNull
        private Set<Statistic> _timerStatistics;
        @NotNull
        private Set<Statistic> _counterStatistics;
        @NotNull
        private Set<Statistic> _gaugeStatistics;
        @NotNull
        private Map<String, Set<Statistic>> _statistics = Collections.emptyMap();
    }
}
