Metrics Aggregator Daemon
=========================

<a href="https://raw.githubusercontent.com/ArpNetworking/metrics-aggregator-daemon/master/LICENSE">
    <img src="https://img.shields.io/hexpm/l/plug.svg"
         alt="License: Apache 2">
</a>
<a href="https://travis-ci.org/ArpNetworking/metrics-aggregator-daemon/">
    <img src="https://travis-ci.org/ArpNetworking/metrics-aggregator-daemon.png?branch=master"
         alt="Travis Build">
</a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics%22%20a%3A%22metrics-aggregator-daemon%22">
    <img src="https://img.shields.io/maven-central/v/com.arpnetworking.metrics/metrics-aggregator-daemon.svg"
         alt="Maven Artifact">
</a>
[![Docker Pulls](https://img.shields.io/docker/pulls/arpnetworking/mad.svg?maxAge=2592000)]()

Aggregates samples into configurable time buckets (e.g. 1 second, 1 minute, etc.) published by metrics client libraries (e.g. [Java](https://github.com/ArpNetworking/metrics-client-java), [NodeJS](https://github.com/ArpNetworking/metrics-client-nodejs), [Ruby](https://github.com/ArpNetworking/metrics-client-ruby), etc.) to compute a variety of statistics. The statistics are reaggregatable and are published together with supporting data to configurable destination(s).


Usage
-----

### Installation

#### Manual
The artifacts from the build are in *metrics-aggregator-daemon/target/appassembler* and should be copied to an appropriate directory on your application host(s).

#### Docker
If you use Docker, we publish a base docker image that makes it easy for you to layer configuration on top of.  Create a Docker image based on the image arpnetworking/mad.  Configuration files are typically located at /opt/mad/config/ with pipeline files located at /opt/mad/config/pipelines.  In addition, you can specify CONFIG_FILE (defaults to /opt/mad/config/config.json), PARAMS (defaults to $CONFIG_FILE), LOGGING_CONFIG (defaults to "-Dlogback.configurationFile=/opt/mad/config/logback.xml"), and JAVA_OPTS (defaults to $LOGGING_CONFIG) environment variables to control startup.

### Execution

In the installation's *bin* directory there are scripts to start Metrics Aggregator Daemon: *mad* (Linux) and *mad.bat* (Windows).  One of these should be executed on system start with appropriate parameters; for example:

    /usr/local/lib/metrics-aggregator-daemon/bin/mad /usr/local/lib/metrics-aggregator-daemon/config/config.json

### Configuration

#### Logging

To customize logging you may provide a [LogBack](http://logback.qos.ch/) configuration file.  To use a custom logging configuration you need to define and export an environment variable before executing *mad*:

    export JAVA_OPTS="-Dlogback.configurationFile=/usr/local/lib/metrics-aggregator-daemon/config/logger.xml"

Where */usr/local/lib/metrics-aggregator-daemon/config/logger.xml* is the path to your logging configuration file.

#### Daemon

The Metrics Aggregator Daemon configuration is specified in a JSON file.  The location of the configuration file is passed to *mad* as a command line argument:

    /usr/local/lib/metrics-aggregator-daemon/config/config.json

The configuration specifies:

* logDirectory - The location of additional logs.  This is independent of the logging configuration.
* pipelinesDirectory - The location of configuration files for each metrics pipeline.
* httpHost - The ip address to bind the http server to.
* httpPort - The port to bind the http server to.
* httpHealthCheckPath - The path in the http server for the health check.
* httpStatusPath - The path in the http server for the status.
* jvmMetricsCollectionInterval - The JVM metrics collection interval in ISO-8601 period notation.
* limiters - Configuration of zero or more limiters by name.
* akkaConfiguration - Configuration of Akka.

For example:

```json
{
    "monitoringCluster": "mad_dev",
    "logDirectory": "/usr/local/lib/metrics-aggregator-daemon/logs",
    "pipelinesDirectory": "/usr/local/lib/metrics-aggregator-daemon/config/pipelines",
    "httpHost": "0.0.0.0",
    "httpPort": 7090,
    "httpHealthCheckPath": "/mad/healthcheck",
    "httpStatusPath": "/mad/status",
    "jvmMetricsCollectionInterval": "PT.5S",
    "akkaConfiguration": {
        "akka": {
            "loggers": [
                "akka.event.slf4j.Slf4jLogger"
            ],
            "loglevel": "DEBUG",
            "stdout-loglevel": "DEBUG",
            "logging-filter": "akka.event.slf4j.Slf4jLoggingFilter",
            "actor": {
                "debug": {
                    "unhandled": "on"
                }
            }
        }
    }
}
```

#### Pipelines

One instance of Metrics Aggregator daemon supports multiple independent services on the same host.  The most basic single application host still typically configures two services: i) the end-user application running on the host, and ii) the system metrics captured by CollectD.  Each of these services is configured as a pipeline in Metrics Aggregator Daemon.  The pipeline defines the name of the service, one or more sources of metrics and one more destinations or sinks for the aggregated statistics.

For example:

```json
{
    "name": "MyApplicationPipeline",
    "serviceName": "MyApplication",
    "sources":
    [
        {
            "type": "com.arpnetworking.metrics.common.sources.FileSource",
            "name": "my_application_source",
            "filePath": "/var/log/my-application-query.log",
            "parser": {
                "type": "com.arpnetworking.metrics.mad.parsers.QueryLogParser"
            }
        }
    ],
    "sinks":
    [
        {
            "type": "com.arpnetworking.tsdcore.sinks.TelemetrySink",
            "name": "my_application_telemetry_sink"
        },
        {
            "type": "com.arpnetworking.tsdcore.sinks.AggregationServerSink",
            "name": "my_application_aggregation_server_sink",
            "serverAddress": "192.168.0.2"
        }
    ]
}
```

Each of the pipeline configuration files should be placed in the *pipelinesDirectory* defined as part of the daemon configuration above.

Development
-----------

To build the service locally you must satisfy these prerequisites:
* [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Docker](http://www.docker.com/) (for [Mac](https://docs.docker.com/docker-for-mac/))

__Note:__ Requires at least Docker for Mac Beta version _Version 1.12.0-rc4-beta19 (build: 10258)_

You will need an account on [Docker Hub](https://hub.docker.com/) and need to provide these credentials in order for the build to pull the base image(s). Please refer to the [Fabric8 Maven Docker Plugin](https://dmp.fabric8.io/#authentication) for how to provide these credentials. To get started quickly you can pass them on the command line via _-Ddocker.pull.username_ and _-Ddocker.pull.password_.

Next, fork the repository, clone and build:

Building:

    metrics-aggregator-daemon> ./mvnw verify

To debug the server during run on port 9000:

    metrics-aggregator-daemon> ./mvnw -Ddebug=true docker:start

To debug the server during integration tests on port 9000:

    metrics-aggregator-daemon> ./mvnw -Ddebug=true verify

To execute performance tests:

    metrics-aggregator-daemon> ./mvnw -PperformanceTest test

To use the local version in your project you must first install it locally:

    metrics-aggregator-daemon> ./mvnw install

You can determine the version of the local build from the pom.xml file.  Using the local version is intended only for testing or development.

You may also need to add the local repository to your build in order to pick-up the local version:

* Maven - Included by default.
* Gradle - Add *mavenLocal()* to *build.gradle* in the *repositories* block.
* SBT - Add *resolvers += Resolver.mavenLocal* into *project/plugins.sbt*.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Groupon Inc., 2014
