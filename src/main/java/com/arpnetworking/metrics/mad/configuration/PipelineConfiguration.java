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
package com.arpnetworking.metrics.mad.configuration;

import akka.actor.ActorSystem;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.metrics.common.kafka.ConsumerDeserializer;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticDeserializer;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice.GuiceAnnotationIntrospector;
import com.fasterxml.jackson.module.guice.GuiceInjectableValues;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Representation of TsdAggregator pipeline configuration. Each pipeline can
 * define one or more sources and one or more sinks.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@Loggable
public final class PipelineConfiguration {

    /**
     * Create an {@link ObjectMapper} for Pipeline configuration.
     *
     * @param injector The Guice {@link Injector} instance.
     * @return An {@link ObjectMapper} for Pipeline configuration.
     */
    public static ObjectMapper createObjectMapper(final Injector injector) {
        final ObjectMapper objectMapper = ObjectMapperFactory.createInstance();

        final SimpleModule module = new SimpleModule("Pipeline");
        module.addDeserializer(Statistic.class, new StatisticDeserializer());
        module.addDeserializer(Consumer.class, new ConsumerDeserializer<>());

        objectMapper.registerModules(module);

        final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
        objectMapper.setInjectableValues(new GuiceInjectableValues(injector));
        objectMapper.setAnnotationIntrospectors(
                new AnnotationIntrospectorPair(
                        guiceIntrospector, objectMapper.getSerializationConfig().getAnnotationIntrospector()),
                new AnnotationIntrospectorPair(
                        guiceIntrospector, objectMapper.getDeserializationConfig().getAnnotationIntrospector()));

        return objectMapper;
    }

    public ActorSystem getActorSystem() {
        return _actorSystem;
    }

    public PeriodicMetrics getPeriodicMetrics() {
        return _periodicMetrics;
    }

    public String getName() {
        return _name;
    }

    public List<Source> getSources() {
        return _sources;
    }

    public List<Sink> getSinks() {
        return _sinks;
    }

    public Set<Duration> getPeriods() {
        return _periods;
    }

    public Set<Statistic> getTimerStatistics() {
        return _timerStatistic;
    }

    public Set<Statistic> getCounterStatistics() {
        return _counterStatistic;
    }

    public Set<Statistic> getGaugeStatistics() {
        return _gaugeStatistic;
    }

    public ImmutableMap<String, Set<Statistic>> getStatistics() {
        return _statistics;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("ActorSystem", _actorSystem)
                .add("PeriodicMetrics", _periodicMetrics)
                .add("Name", _name)
                .add("Sources", _sources)
                .add("Sinks", _sinks)
                .add("Periods", _periods)
                .add("TimerStatistic", _timerStatistic)
                .add("CounterStatistic", _counterStatistic)
                .add("GaugeStatistic", _gaugeStatistic)
                .toString();
    }

    private PipelineConfiguration(final Builder builder) {
        _actorSystem = builder._actorSystem;
        _periodicMetrics = builder._periodicMetrics;
        _name = builder._name;
        _sources = ImmutableList.copyOf(builder._sources);
        _sinks = ImmutableList.copyOf(builder._sinks);
        _periods = ImmutableSet.copyOf(builder._periods);
        _timerStatistic = ImmutableSet.copyOf(builder._timerStatistics);
        _counterStatistic = ImmutableSet.copyOf(builder._counterStatistics);
        _gaugeStatistic = ImmutableSet.copyOf(builder._gaugeStatistics);
        _statistics = ImmutableMap.copyOf(builder._statistics);
    }

    private final ActorSystem _actorSystem;
    private final PeriodicMetrics _periodicMetrics;
    private final String _name;
    private final ImmutableList<Source> _sources;
    private final ImmutableList<Sink> _sinks;
    private final ImmutableSet<Duration> _periods;
    private final ImmutableSet<Statistic> _timerStatistic;
    private final ImmutableSet<Statistic> _counterStatistic;
    private final ImmutableSet<Statistic> _gaugeStatistic;
    private final ImmutableMap<String, Set<Statistic>> _statistics;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * .{@link PipelineConfiguration}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static class Builder extends OvalBuilder<PipelineConfiguration> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(PipelineConfiguration::new);
        }

        /**
         * The Akka {@link ActorSystem}. Cannot be null.
         *
         * @param value The Akka {@link ActorSystem}.
         * @return This instance of {@link Builder}.
         */
        public Builder setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return this;
        }

        /**
         * The {@link PeriodicMetrics}. Cannot be null.
         *
         * @param value The {@link PeriodicMetrics}.
         * @return This instance of {@link Builder}.
         */
        public Builder setPeriodicMetrics(final PeriodicMetrics value) {
            _periodicMetrics = value;
            return this;
        }

        /**
         * The name of the pipeline. Cannot be null or empty.
         *
         * @param value The name of the pipeline.
         * @return This instance of {@link Builder}.
         */
        public Builder setName(final String value) {
            _name = value;
            return this;
        }

        /**
         * The query log sources. Cannot be null.
         *
         * @param value The query log sources.
         * @return This instance of {@link Builder}.
         */
        public Builder setSources(final List<Source> value) {
            _sources = value;
            return this;
        }

        /**
         * The sinks. Cannot be null.
         *
         * @param value The sinks.
         * @return This instance of {@link Builder}.
         */
        public Builder setSinks(final List<Sink> value) {
            _sinks = value;
            return this;
        }

        /**
         * The aggregation periods. Cannot be null or empty. Default is one
         * second and five minute periods.
         *
         * @param value The sinks.
         * @return This instance of {@link Builder}.
         */
        public Builder setPeriods(final Set<Duration> value) {
            _periods = value;
            return this;
        }

        /**
         * The statistics to compute for all timers. Cannot be null or empty.
         * Default is TP50, TP90, TP99, Mean and Count.
         *
         * @param value The timer statistics.
         * @return This instance of {@link Builder}.
         */
        public Builder setTimerStatistics(final Set<Statistic> value) {
            _timerStatistics = value;
            return this;
        }

        /**
         * The statistics to compute for all counters. Cannot be null or empty.
         * Default is Mean, Sum and Count.
         *
         * @param value The counter statistics.
         * @return This instance of {@link Builder}.
         */
        public Builder setCounterStatistics(final Set<Statistic> value) {
            _counterStatistics = value;
            return this;
        }

        /**
         * The statistics to compute for all gauges. Cannot be null or empty.
         * Default is Min, Max and Mean.
         *
         * @param value The gauge statistics.
         * @return This instance of {@link Builder}.
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
        @JacksonInject
        private ActorSystem _actorSystem;
        @NotNull
        @JacksonInject
        private PeriodicMetrics _periodicMetrics;
        @NotNull
        @NotEmpty
        private String _name;
        @NotNull
        private List<Source> _sources = Collections.emptyList();
        @NotNull
        private List<Sink> _sinks = Collections.emptyList();
        @NotNull
        @NotEmpty
        private Set<Duration> _periods = Sets.newHashSet(
                Duration.ofSeconds(1),
                Duration.ofMinutes(1));
        @NotNull
        @NotEmpty
        private Set<Statistic> _timerStatistics = Sets.newHashSet(
                STATISTIC_FACTORY.getStatistic("median"),
                STATISTIC_FACTORY.getStatistic("tp90"),
                STATISTIC_FACTORY.getStatistic("tp99"),
                STATISTIC_FACTORY.getStatistic("mean"),
                STATISTIC_FACTORY.getStatistic("count"));
        @NotNull
        @NotEmpty
        private Set<Statistic> _counterStatistics = Sets.newHashSet(
                STATISTIC_FACTORY.getStatistic("mean"),
                STATISTIC_FACTORY.getStatistic("sum"),
                STATISTIC_FACTORY.getStatistic("count"));
        @NotNull
        @NotEmpty
        private Set<Statistic> _gaugeStatistics = Sets.newHashSet(
                STATISTIC_FACTORY.getStatistic("min"),
                STATISTIC_FACTORY.getStatistic("max"),
                STATISTIC_FACTORY.getStatistic("mean"));
        @NotNull
        private Map<String, Set<Statistic>> _statistics = Collections.emptyMap();
    }
}
