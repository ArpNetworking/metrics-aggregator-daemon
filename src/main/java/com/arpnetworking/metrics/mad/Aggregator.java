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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
        final int instances = 2 * Runtime.getRuntime().availableProcessors();
        LOGGER.info()
                .setMessage("Launching aggregator")
                .addData("aggregator", this)
                .addData("actors", instances)
                .log();
        for (int i = 0; i < instances; ++i) {
            _actors.add(_actorSystem.actorOf(Actor.props(this)));
        }
    }

    @Override
    public synchronized void shutdown() {
        LOGGER.debug()
                .setMessage("Stopping aggregator")
                .addData("aggregator", this)
                .log();

        if (!_actors.isEmpty()) {
            try {
                // Start aggregator shutdown
                final List<CompletableFuture<Boolean>> aggregatorShutdown = shutdownActors(_actors, SHUTDOWN_MESSAGE);

                // Wait for shutdown
                CompletableFutures.allOf(aggregatorShutdown).get(
                        SHUTDOWN_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS);

                // Report any failures
                final boolean success = aggregatorShutdown.stream()
                        .map(f -> f.getNow(false))
                        .reduce(Boolean::logicalAnd)
                        .orElse(true);
                if (!success) {
                    LOGGER.error()
                            .setMessage("Failed stopping one or more aggregator actors")
                            .log();
                }
            } catch (final InterruptedException e) {
                LOGGER.warn()
                        .setMessage("Interrupted stopping aggregator actors")
                        .addData("aggregatorActors", _actors)
                        .log();
            } catch (final TimeoutException | ExecutionException e) {
                LOGGER.error()
                        .setMessage("Aggregator actors stop timed out or failed")
                        .addData("aggregatorActors", _actors)
                        .addData("timeout", SHUTDOWN_TIMEOUT)
                        .setThrowable(e)
                        .log();
            }
        }

        // Clear state
        _actors.clear();
    }

    private static final Random R = new Random();

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

        // TODO(ville): Should the record contain a key instead of just annotations?
        // ^ This raises the bigger question of metric name as part of the key.
        // (at the moment it's not and thus able to take advantage of same key-space across metrics in bucket)
        //
        // The hashing can be improved in a number of ways:
        // * The hash code could use MD5 or Murmur to generate a better distribution.
        // * The modulo itself introduces a small skew to certain actor partitions.
        // * More fundamentally, the workload per partition varies and scale is subject
        //   to the highest partition's workload. We may need to consider a different
        //   model entirely.
        final Record record = (Record) event;
        final int actorIndex = (record.getDimensions().hashCode() & 0x7FFFFFFF) % _actors.size();
        _actors.get(actorIndex).tell(record, ActorRef.noSender());
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

    private List<ActorRef> createActors(final Key key, final ActorRef aggregatorActor) {
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
            periodWorkerList.add(
                    _actorSystem.actorOf(
                            Props.create(
                                    PeriodWorker.class,
                                    aggregatorActor,
                                    key,
                                    period,
                                    _idleTimeout,
                                    bucketBuilder,
                                    _periodicMetrics)));
        }
        LOGGER.debug()
                .setMessage("Created period worker actors")
                .addData("key", key)
                .addData("periodWorkersSize", periodWorkerList.size())
                .log();
        return periodWorkerList;
    }

    private List<CompletableFuture<Boolean>> shutdownActors(final List<ActorRef> actors, final Object message) {
        final List<CompletableFuture<Boolean>> shutdownFutures = new ArrayList<>();
        for (final ActorRef actorRef : actors) {
            shutdownFutures.add(
                    Patterns.gracefulStop(
                            actorRef,
                            SHUTDOWN_TIMEOUT,
                            message).toCompletableFuture());
        }
        return shutdownFutures;
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

        _periodicMetrics.registerPolledMetric(m -> {
            // TODO(vkoskela): There needs to be a way to deregister these callbacks
            // This is not an immediate issue since new Aggregator instances are
            // only created when pipelines are reloaded. To avoid recording values
            // for dead pipelines this explicitly avoids recording zeroes.
            final long samples = _receivedSamples.getAndSet(0);
            if (samples > 0) {
                m.recordGauge("aggregator/samples", samples);
            }
        });
    }

    private final List<ActorRef> _actors = new ArrayList<>();

    // WARNING: Consider carefully the volume of samples recorded.
    // PeriodicMetrics reduces the number of scopes creates, but each sample is
    // still stored in-memory until it is flushed.
    private final PeriodicMetrics _periodicMetrics;
    private final AtomicLong _receivedSamples = new AtomicLong(0);

    private final ActorSystem _actorSystem;
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
    private static final String SHUTDOWN_MESSAGE = "shutdown";
    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregator.class);

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
        static Props props(final Aggregator aggregator) {
            return Props.create(Actor.class, aggregator);
        }

        @Override
        public void preStart() {
            _aggregator._periodicMetrics.recordCounter("actors/aggregator/started", 1);
        }

        @Override
        public void postStop() {
            _aggregator._periodicMetrics.recordCounter("actors/aggregator/stopped", 1);
        }

        @Override
        public void preRestart(final Throwable reason, final Optional<Object> message) {
            _aggregator._periodicMetrics.recordCounter("actors/aggregator/restarted", 1);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Record.class, this::record)
                    .match(PeriodWorkerIdle.class, this::idleWorker)
                    .matchEquals(SHUTDOWN_MESSAGE, ignored -> this.shutdown())
                    .build();
        }

        private void shutdown() {
            try {
                // Start period worker shutdown
                final List<CompletableFuture<Boolean>> periodWorkerShutdown = new ArrayList<>();
                for (final List<ActorRef> workers : _periodWorkerActors.values()) {
                    periodWorkerShutdown.addAll(_aggregator.shutdownActors(workers, PoisonPill.getInstance()));
                }

                // Wait for shutdown
                CompletableFutures.allOf(periodWorkerShutdown).get(
                        SHUTDOWN_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS);

                // Report any failures
                final boolean success = periodWorkerShutdown.stream()
                        .map(f -> f.getNow(false))
                        .reduce(Boolean::logicalAnd)
                        .orElse(true);
                if (!success) {
                    LOGGER.error()
                            .setMessage("Failed stopping one or more period worker actors")
                            .addData("aggregatorActor", self())
                            .log();
                }
            } catch (final InterruptedException e) {
                LOGGER.warn()
                        .setMessage("Interrupted stopping period worker actors")
                        .addData("aggregatorActor", self())
                        .log();
            } catch (final TimeoutException | ExecutionException e) {
                LOGGER.error()
                        .setMessage("Period worker actors stop timed out or failed")
                        .addData("aggregatorActor", self())
                        .addData("timeout", SHUTDOWN_TIMEOUT)
                        .setThrowable(e)
                        .log();
            }
            _periodWorkerActors.clear();
        }

        private void idleWorker(final PeriodWorkerIdle idle) {
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
        }

        private void record(final Record record) {
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
            _aggregator._receivedSamples.addAndGet(samples);

            LOGGER.trace()
                    .setMessage("Sending record to aggregation actor")
                    .addData("record", record)
                    .addData("key", key)
                    .log();

            List<ActorRef> periodWorkers = _periodWorkerActors.get(key);
            if (periodWorkers == null) {
                periodWorkers = _aggregator.createActors(key, self());
                _periodWorkerActors.put(key, periodWorkers);
            }
            for (final ActorRef periodWorkerActor : periodWorkers) {
                periodWorkerActor.tell(record, self());
            }
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
         * Set the idle timeout for workers. Worker actors which do not receive
         * a record within this time period are stopped. Cannot be null.
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
