/**
 * Copyright 2014 Brandon Arp
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

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.Http;
import akka.http.javadsl.IncomingConnection;
import akka.http.javadsl.ServerBinding;
import akka.stream.ActorFlowMaterializer;
import akka.stream.ActorFlowMaterializerSettings;
import ch.qos.logback.classic.LoggerContext;
import com.arpnetworking.configuration.jackson.DynamicConfiguration;
import com.arpnetworking.configuration.jackson.JsonNodeFileSource;
import com.arpnetworking.configuration.triggers.FileTrigger;
import com.arpnetworking.http.Routes;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.impl.TsdLogSink;
import com.arpnetworking.metrics.impl.TsdMetricsFactory;
import com.arpnetworking.metrics.jvm.JvmMetricsRunnable;
import com.arpnetworking.metrics.mad.configuration.AggregatorConfiguration;
import com.arpnetworking.metrics.mad.configuration.PipelineConfiguration;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.utility.Configurator;
import com.arpnetworking.utility.Launchable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Future;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class containing entry point for Metrics Data Aggregator (MAD).
 *
 * @author Brandon Arp (barp at groupon dot com)
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public final class Main implements Launchable {

    /**
     * Entry point for Metrics Aggregator Daemon (MAD).
     *
     * @param args the command line arguments
     */
    public static void main(final String[] args) {
        LOGGER.info().setMessage("Launching mad").log();

        // Global initialization
        Thread.setDefaultUncaughtExceptionHandler(
                (thread, throwable) -> {
                    System.err.println("Unhandled exception! exception: " + throwable.toString());
                    throwable.printStackTrace(System.err);
                });

        Thread.currentThread().setUncaughtExceptionHandler(
                (thread, throwable) -> LOGGER.error()
                        .setMessage("Unhandled exception!")
                        .setThrowable(throwable)
                        .log()
        );

        System.setProperty("org.vertx.logger-delegate-factory-class-name", "org.vertx.java.core.logging.impl.SLF4JLogDelegateFactory");

        // Run the tsd aggregator
        if (args.length != 1) {
            throw new RuntimeException("No configuration file specified");
        }
        LOGGER.debug()
                .setMessage("Loading configuration")
                .addData("file", args[0])
                .log();

        final File configurationFile = new File(args[0]);
        final Configurator<Main, AggregatorConfiguration> configurator =
                new Configurator<>(Main::new, AggregatorConfiguration.class);
        final ObjectMapper objectMapper = AggregatorConfiguration.createObjectMapper();
        final DynamicConfiguration configuration = new DynamicConfiguration.Builder()
                .setObjectMapper(objectMapper)
                .addSourceBuilder(
                        new JsonNodeFileSource.Builder()
                                .setObjectMapper(objectMapper)
                                .setFile(configurationFile))
                .addTrigger(
                        new FileTrigger.Builder()
                                .setFile(configurationFile)
                                .build())
                .addListener(configurator)
                .build();

        configuration.launch();

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            LOGGER.info().setMessage("Stopping mad").log();
                            configuration.shutdown();
                            configurator.shutdown();

                            LOGGER.info().setMessage("Exiting mad").log();
                            final LoggerContext context = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
                            context.stop();
                        },
                        "TsdAggregatorShutdownHook"));
    }

    /**
     * Public constructor.
     *
     * @param configuration Instance of <code>TsdAggregatorConfiguration</code>.
     */
    public Main(final AggregatorConfiguration configuration) {
        _configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void launch() {
        _actorSystem = launchAkka();
        final Injector injector = launchGuice(_actorSystem);
        launchActors(injector);
        launchPipelines(injector);
        launchJvmMetricsCollector(injector);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdown() {
        shutdownJvmMetricsCollector();
        shutdownPipelines();
        shutdownActors();
        shutdownGuice();
        shutdownAkka();
    }

    private void launchJvmMetricsCollector(final Injector injector) {
        LOGGER.info().setMessage("Launching JVM metrics collector.").log();
        final Runnable runnable = new JvmMetricsRunnable.Builder()
                .setMetricsFactory(injector.getInstance(MetricsFactory.class))
                .build();
        _jvmMetricsCollector = Executors.newSingleThreadScheduledExecutor((r) -> new Thread(r, "JVMMetricsCollector"));
        _jvmMetricsCollector.scheduleAtFixedRate(
                runnable,
                INITIAL_DELAY_IN_MILLIS,
                _configuration.getJvmMetricsCollectionInterval().toStandardDuration().getMillis(),
                TIME_UNIT);
    }

    private void launchPipelines(final Injector injector) {
        LOGGER.info().setMessage("Launching pipelines").log();
        _pipelinesLaunchable = new PipelinesLaunchable(
                PipelineConfiguration.createObjectMapper(injector),
                _configuration.getPipelinesDirectory());
        _pipelinesLaunchable.launch();
    }

    private void launchActors(final Injector injector) {
        LOGGER.info().setMessage("Launching actors").log();

        // Retrieve the actor system
        final ActorSystem actorSystem = injector.getInstance(ActorSystem.class);

        // Create the status actor
        actorSystem.actorOf(Props.create(Status.class), "status");

        final ActorFlowMaterializerSettings materializerSettings = ActorFlowMaterializerSettings.create(actorSystem);
        final ActorFlowMaterializer materializer = ActorFlowMaterializer.create(materializerSettings, actorSystem);

        // Create and bind Http server
        final Routes routes = new Routes(actorSystem, injector.getInstance(MetricsFactory.class));
        final Http http = Http.get(actorSystem);
        final akka.stream.javadsl.Source<IncomingConnection, Future<ServerBinding>> binding = http.bind(
                _configuration.getHttpHost(),
                _configuration.getHttpPort(),
                materializer);
        binding.runForeach(
                connection -> connection.handleWithAsyncHandler(routes, materializer),
                materializer);
    }

    private Injector launchGuice(final ActorSystem actorSystem) {
        LOGGER.info().setMessage("Launching guice").log();

        // Create directories
        if (_configuration.getLogDirectory().mkdirs()) {
            LOGGER.info()
                    .setMessage("Created log directory")
                    .addData("directory", _configuration.getLogDirectory())
                    .log();
        }
        if (_configuration.getPipelinesDirectory().mkdirs()) {
            LOGGER.info()
                    .setMessage("Created pipelines directory")
                    .addData("directory", _configuration.getPipelinesDirectory())
                    .log();
        }

        // Instantiate the metrics factory
        final MetricsFactory metricsFactory = new TsdMetricsFactory.Builder()
                .setClusterName(_configuration.getMonitoringCluster())
                .setServiceName("mad")
                .setSinks(
                        Collections.singletonList(
                                new TsdLogSink.Builder()
                                        .setDirectory(_configuration.getLogDirectory())
                                        .setName("mad-query")
                                        .build()))
                .build();

        // Instantiate Guice
        return Guice.createInjector(new MainModule(actorSystem, metricsFactory));
    }

    private ActorSystem launchAkka() {
        final Config akkaConfiguration = ConfigFactory.parseMap(_configuration.getAkkaConfiguration());
        return ActorSystem.create("MAD", ConfigFactory.load(akkaConfiguration));
    }

    private void shutdownJvmMetricsCollector() {
        LOGGER.info().setMessage("Stopping JVM metrics collection").log();
        if (_jvmMetricsCollector != null) {
            _jvmMetricsCollector.shutdown();
        }
    }

    private void shutdownPipelines() {
        LOGGER.info().setMessage("Stopping pipelines").log();
        _pipelinesLaunchable.shutdown();
    }

    private void shutdownActors() {
        LOGGER.info().setMessage("Stopping actors").log();
    }

    // TODO(vkoskela): Fix Akka shutdown logic. [ISSUE-?]
    @SuppressWarnings("deprecation")
    private void shutdownAkka() {
        LOGGER.info().setMessage("Stopping akka").log();

        if (_actorSystem != null) {
            _actorSystem.shutdown();
            _actorSystem.awaitTermination();
            _actorSystem = null;
        }
    }

    private void shutdownGuice() {
        LOGGER.info().setMessage("Stopping guice").log();
    }

    private final AggregatorConfiguration _configuration;

    private volatile PipelinesLaunchable _pipelinesLaunchable;
    private volatile ScheduledExecutorService _jvmMetricsCollector;

    private volatile ActorSystem _actorSystem;

    private static final Long INITIAL_DELAY_IN_MILLIS = 0L;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final class PipelinesLaunchable implements Launchable, Runnable {

        private PipelinesLaunchable(final ObjectMapper objectMapper, final File directory) {
            _objectMapper = objectMapper;
            _directory = directory;
            _fileToPipelineLaunchables = Maps.newConcurrentMap();
        }

        @Override
        public synchronized void launch() {
            _pipelinesExecutor = Executors.newSingleThreadScheduledExecutor((runnable) -> new Thread(runnable, "PipelineExecutor"));
            _pipelinesExecutor.scheduleAtFixedRate(
                    this,
                    0,  // initial delay
                    1,  // interval
                    TimeUnit.MINUTES);
        }

        @Override
        public synchronized void shutdown() {
            _pipelinesExecutor.shutdown();
            try {
                _pipelinesExecutor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                LOGGER.warn("Unable to shutdown pipeline executor", e);
            }
            _pipelinesExecutor = null;

            _fileToPipelineLaunchables.keySet()
                    .stream()
                    .forEach(this::shutdownPipeline);
            _fileToPipelineLaunchables.clear();
        }

        @Override
        public void run() {
            final boolean exists = _directory.exists() && _directory.isDirectory();
            if (exists) {
                final Set<File> missingFiles = Sets.newHashSet(_fileToPipelineLaunchables.keySet());
                for (final File file : MoreObjects.firstNonNull(_directory.listFiles(), EMPTY_FILE_ARRAY)) {
                    missingFiles.remove(file);
                    if (!_fileToPipelineLaunchables.containsKey(file)) {
                        launchPipeline(file);
                    }
                }
                missingFiles.forEach(this::shutdownPipeline);
            } else {
                _fileToPipelineLaunchables.keySet().forEach(this::shutdownPipeline);
            }
        }

        private void launchPipeline(final File file) {
            LOGGER.debug()
                    .setMessage("Creating pipeline")
                    .addData("configuration", file)
                    .log();

            final Configurator<Pipeline, PipelineConfiguration> pipelineConfigurator =
                    new Configurator<>(Pipeline::new, PipelineConfiguration.class);
            final DynamicConfiguration pipelineConfiguration = new DynamicConfiguration.Builder()
                    .setObjectMapper(_objectMapper)
                    .addSourceBuilder(
                            new JsonNodeFileSource.Builder()
                                    .setObjectMapper(_objectMapper)
                                    .setFile(file))
                    .addTrigger(
                            new FileTrigger.Builder()
                                    .setFile(file)
                                    .build())
                    .addListener(pipelineConfigurator)
                    .build();

            LOGGER.debug()
                    .setMessage("Launching pipeline")
                    .addData("pipeline", pipelineConfiguration)
                    .log();

            pipelineConfiguration.launch();

            _fileToPipelineLaunchables.put(file, ImmutableList.of(pipelineConfigurator, pipelineConfiguration));
        }

        private void shutdownPipeline(final File file) {
            LOGGER.debug()
                    .setMessage("Stopping pipeline")
                    .addData("pipeline", file)
                    .log();

            _fileToPipelineLaunchables.remove(file).forEach(com.arpnetworking.utility.Launchable::shutdown);
        }

        private final ObjectMapper _objectMapper;
        private final File _directory;
        private final Map<File, List<Launchable>> _fileToPipelineLaunchables;

        private ScheduledExecutorService _pipelinesExecutor;

        private static final File[] EMPTY_FILE_ARRAY = new File[0];
    }

    private static final class MainModule extends AbstractModule {

        MainModule(final ActorSystem actorSystem, final MetricsFactory metricsFactory) {
            this._actorSystem = actorSystem;
            this._metricsFactory = metricsFactory;
        }

        @Override
        public void configure() {
            bind(ActorSystem.class).toInstance(_actorSystem);
            bind(MetricsFactory.class).toInstance(_metricsFactory);
        }

        private final ActorSystem _actorSystem;
        private final MetricsFactory _metricsFactory;
    }
}
