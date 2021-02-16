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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.java.util.concurrent.CompletableFutures;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Performs aggregation of {@link Record} instances per {@link Duration}.
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
        _actor = _actorSystem.actorOf(Actor.props(this));
    }

    @Override
    public synchronized void shutdown() {
        LOGGER.debug()
                .setMessage("Stopping aggregator")
                .addData("aggregator", this)
                .log();

        if (_actor != null) {
            try {
                if (!Patterns.gracefulStop(
                        _actor,
                        SHUTDOWN_TIMEOUT,
                        ShutdownAggregator.getInstance()).toCompletableFuture().get(
                        SHUTDOWN_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS)) {
                    LOGGER.warn()
                            .setMessage("Failed stopping aggregator actor")
                            .addData("actor", _actor)
                            .log();
                }
            } catch (final InterruptedException e) {
                LOGGER.warn()
                        .setMessage("Interrupted stopping aggregator actor")
                        .addData("actor", _actor)
                        .log();
            } catch (final TimeoutException | ExecutionException e) {
                LOGGER.error()
                        .setMessage("Aggregator actor stop timed out or failed")
                        .addData("actor", _actor)
                        .addData("timeout", SHUTDOWN_TIMEOUT)
                        .setThrowable(e)
                        .log();
            }
            _actor = null;
        }
    }

    @Override
    @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
    public void notify(final Observable observable, final Object event) {
        if (!(event instanceof Record)) {
            LOGGER.error()
                    .setMessage("Observed unsupported event")
                    .addData("event", event)
                    .log();
            return;
        }

        final Record record = (Record) event;
        _actor.tell(record, ActorRef.noSender());
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

    private ImmutableSet<Statistic> computeDependentStatistics(final ImmutableSet<Statistic> statistics) {
        final ImmutableSet.Builder<Statistic> builder = ImmutableSet.builder();
        for (final Statistic statistic : statistics) {
            statistic.getDependencies().stream().filter(dependency -> !statistics.contains(dependency)).forEach(builder::add);
        }
        return builder.build();
    }

    private Aggregator(final Builder builder) {
        _actorSystem = builder._actorSystem;
        _periodicMetrics = builder._periodicMetrics;
        _periods = ImmutableSet.copyOf(builder._periods);
        _idleTimeout = builder._idleTimeout;
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

    private ActorRef _actor;

    private final ActorSystem _actorSystem;
    private final PeriodicMetrics _periodicMetrics;
    private final ImmutableSet<Duration> _periods;
    private final Duration _idleTimeout;
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

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregator.class);

    private static final class ShutdownAggregator implements Serializable {
        /**
         * Gets the singleton instance.
         *
         * @return singleton instance
         */
        public static ShutdownAggregator getInstance() {
            return INSTANCE;
        }

        private static final ShutdownAggregator INSTANCE = new ShutdownAggregator();
        private static final long serialVersionUID = 1L;
    }

    static final class PeriodWorkerIdle implements Serializable {

        PeriodWorkerIdle(final Key key) {
            _key = key;
        }

        public Key getKey() {
            return _key;
        }

        private final Key _key;

        private static final long serialVersionUID = 1L;
    }

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends AbstractActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @return A new {@link Props}
         */
        /* package private */
        static Props props(final Aggregator aggregator) {
            return Props.create(Actor.class, aggregator);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Record.class, record -> {
                        final Key key = new DefaultKey(record.getDimensions());

                        long samples = 0;
                        for (final Metric metric : record.getMetrics().values()) {
                            samples += metric.getValues().size();
                            final List<CalculatedValue<?>> countStatistic =
                                    metric.getStatistics().get(STATISTIC_FACTORY.getStatistic("count"));
                            if (countStatistic != null) {
                                samples += countStatistic.stream()
                                        .map(s -> s.getValue().getValue())
                                        .reduce(Double::sum)
                                        .orElse(0.0d);
                            }
                        }
                        _aggregator._periodicMetrics.recordGauge("aggregator/samples", samples);

                        LOGGER.trace()
                                .setMessage("Sending record to aggregation actor")
                                .addData("record", record)
                                .addData("key", key)
                                .log();

                        List<ActorRef> periodWorkers = _periodWorkerActors.get(key);
                        if (periodWorkers == null) {
                            periodWorkers = createActors(key);
                            _periodWorkerActors.put(key, periodWorkers);
                        }

                        for (final ActorRef periodWorkerActor : periodWorkers) {
                            periodWorkerActor.tell(record, self());
                        }
                    })
                    .match(PeriodWorkerIdle.class, idle -> {
                        final Key key = idle.getKey();
                        final List<ActorRef> workers = _periodWorkerActors.remove(key);
                        if (workers != null) {
                            LOGGER.debug()
                                    .setMessage("Stopping idle period worker actors")
                                    .addData("key", key)
                                    .addData("worker_count", workers.size())
                                    .log();

                            for (final ActorRef worker : workers) {
                                worker.tell(PoisonPill.getInstance(), self());
                            }
                        }
                    })
                    .match(ShutdownAggregator.class, ignored -> {
                        final List<CompletableFuture<Object>> shutdownStages = new ArrayList<>();
                        for (final List<ActorRef> actorRefList : _periodWorkerActors.values()) {
                            for (final ActorRef actorRef : actorRefList) {
                                shutdownStages.add(
                                        Patterns.ask(
                                                actorRef,
                                                PoisonPill.getInstance(),
                                                SHUTDOWN_TIMEOUT).toCompletableFuture());
                            }
                        }
                        _periodWorkerActors.clear();
                        try {
                            CompletableFutures.allOf(shutdownStages).get(
                                    SHUTDOWN_TIMEOUT.toMillis(),
                                    TimeUnit.MILLISECONDS);
                        } catch (final InterruptedException e) {
                            LOGGER.warn()
                                    .setMessage("Interrupted waiting for actors to shutdown")
                                    .log();
                        } catch (final TimeoutException | ExecutionException e) {
                            LOGGER.error()
                                    .setMessage("Waiting for actors to shutdown timed out or failed")
                                    .setThrowable(e)
                                    .log();
                        }
                    })
                    .build();
        }

        private List<ActorRef> createActors(final Key key) {
            final List<ActorRef> periodWorkerList = Lists.newArrayListWithExpectedSize(_aggregator._periods.size());
            for (final Duration period : _aggregator._periods) {
                final Bucket.Builder bucketBuilder = new Bucket.Builder()
                        .setKey(key)
                        .setSpecifiedCounterStatistics(_aggregator._specifiedCounterStatistics)
                        .setSpecifiedGaugeStatistics(_aggregator._specifiedGaugeStatistics)
                        .setSpecifiedTimerStatistics(_aggregator._specifiedTimerStatistics)
                        .setDependentCounterStatistics(_aggregator._dependentCounterStatistics)
                        .setDependentGaugeStatistics(_aggregator._dependentGaugeStatistics)
                        .setDependentTimerStatistics(_aggregator._dependentTimerStatistics)
                        .setSpecifiedStatistics(_aggregator._cachedSpecifiedStatistics)
                        .setDependentStatistics(_aggregator._cachedDependentStatistics)
                        .setPeriod(period)
                        .setSink(_aggregator._sink);
                periodWorkerList.add(
                        _aggregator._actorSystem.actorOf(
                                Props.create(
                                        PeriodWorker.class,
                                        this.self(),
                                        key,
                                        period,
                                        _aggregator._idleTimeout,
                                        bucketBuilder,
                                        _aggregator._periodicMetrics)));
            }
            LOGGER.debug()
                    .setMessage("Created period worker actors")
                    .addData("key", key)
                    .addData("periodWorkersSize", periodWorkerList.size())
                    .log();
            return periodWorkerList;
        }

        /**
         * Constructor.
         *
         * @param aggregator The {@link Aggregator} to implement.
         */
        /* package private */ Actor(final Aggregator aggregator) {
            _aggregator = aggregator;
        }

        private final Aggregator _aggregator;
        private final Map<Key, List<ActorRef>> _periodWorkerActors = Maps.newHashMap();
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link Aggregator}.
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
         * @return This {@link Builder} instance.
         */
        public Builder setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return this;
        }

        /**
         * Set the {@link PeriodicMetrics}. Cannot be null.
         *
         * @param value The {@link PeriodicMetrics}.
         * @return This {@link Builder} instance.
         */
        public Builder setPeriodicMetrics(final PeriodicMetrics value) {
            _periodicMetrics = value;
            return this;
        }

        /**
         * Set the sink. Cannot be null or empty.
         *
         * @param value The sink.
         * @return This {@link Builder} instance.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * Set the periods. Cannot be null or empty.
         *
         * @param value The periods.
         * @return This {@link Builder} instance.
         */
        public Builder setPeriods(final Set<Duration> value) {
            _periods = value;
            return this;
        }

        /**
         * Set the idle timeout for workers. Cannot be null.
         *
         * @param value The idle timeout.
         * @return This {@link Builder} instance.
         */
        public Builder setIdleTimeout(final Duration value) {
            _idleTimeout = value;
            return this;
        }

        /**
         * Set the timer statistics. Cannot be null or empty.
         *
         * @param value The timer statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setTimerStatistics(final Set<Statistic> value) {
            _timerStatistics = value;
            return this;
        }

        /**
         * Set the counter statistics. Cannot be null or empty.
         *
         * @param value The counter statistics.
         * @return This {@link Builder} instance.
         */
        public Builder setCounterStatistics(final Set<Statistic> value) {
            _counterStatistics = value;
            return this;
        }

        /**
         * Set the gauge statistics. Cannot be null or empty.
         *
         * @param value The gauge statistics.
         * @return This {@link Builder} instance.
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
         * @return This instance of {@link Builder}.
         */
        public Builder setStatistics(final Map<String, Set<Statistic>> value) {
            _statistics = value;
            return this;
        }

        @NotNull
        private ActorSystem _actorSystem;
        @NotNull
        private PeriodicMetrics _periodicMetrics;
        @NotNull
        private Sink _sink;
        @NotNull
        private Set<Duration> _periods;
        @NotNull
        private Duration _idleTimeout;
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
