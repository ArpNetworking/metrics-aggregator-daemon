Metrics Aggregator Daemon
=========================

<a href="https://raw.githubusercontent.com/ArpNetworking/metrics-aggregator-daemon/master/LICENSE">
    <img src="https://img.shields.io/hexpm/l/plug.svg"
         alt="License: Apache 2">
</a>
<a href="https://travis-ci.com/ArpNetworking/metrics-aggregator-daemon">
    <img src="https://travis-ci.com/ArpNetworking/metrics-aggregator-daemon.svg?branch=master"
         alt="Travis Build">
</a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics%22%20a%3A%22metrics-aggregator-daemon%22">
    <img src="https://img.shields.io/maven-central/v/com.arpnetworking.metrics/metrics-aggregator-daemon.svg"
         alt="Maven Artifact">
</a>
<a href="https://hub.docker.com/r/arpnetworking/mad">
    <img src="https://img.shields.io/docker/pulls/arpnetworking/mad.svg" alt="Docker">
</a>

Aggregates samples into configurable time buckets (e.g. 1 second, 1 minute, etc.) published by metrics client libraries
(e.g. [Java](https://github.com/ArpNetworking/metrics-client-java),
[NodeJS](https://github.com/ArpNetworking/metrics-client-nodejs),
[Ruby](https://github.com/ArpNetworking/metrics-client-ruby), etc.) to compute a variety of statistics. The statistics
are reaggregatable and are published together with supporting data to configurable destination(s).


Usage
-----

### Installation

#### Manual
The artifacts from the build are in *metrics-aggregator-daemon/target/appassembler* and should be copied to an
appropriate directory on your application host(s).

#### Docker
If you use Docker, we publish a base docker image that makes it easy for you to layer configuration on top of.  Create
a Docker image based on the image arpnetworking/mad.  Configuration files are typically located at /opt/mad/config/
with pipeline files located at /opt/mad/config/pipelines.  In addition, you can specify CONFIG_FILE (defaults to
/opt/mad/config/config.conf), LOGGING_CONFIG (defaults to "-Dlogback.configurationFile=/opt/mad/config/logback.xml"),
and JAVA_OPTS (defaults to "") environment variables to control startup.

### Execution

In the installation's *bin* directory there are scripts to start Metrics Aggregator Daemon: *mad* (Linux) and
*mad.bat* (Windows).  One of these should be executed on system start with appropriate parameters; for example:

    /usr/local/lib/metrics-aggregator-daemon/bin/mad /usr/local/lib/metrics-aggregator-daemon/config/config.conf

### Configuration

#### Logging

To customize logging you may provide a [LogBack](http://logback.qos.ch/) configuration file. The project ships with
`logback.xml` which writes logs to rotated files and with `logback-console.xml` which writes logs to STDOUT.

Outside of Docker, set the `JAVA_OPTS` environment variable to configure logging:

    export JAVA_OPTS="-Dlogback.configurationFile=/usr/local/lib/metrics-aggregator-daemon/config/logback-console.xml"

Where */usr/local/lib/metrics-aggregator-daemon/config/logger-console.xml* is the path to your logging configuration file.

Under Docker, set the `LOGBACK_CONFIG` environment variable to configure logging:

    docker run -e LOGBACK_CONFIG=/opt/mad/config/logack-console.xml arpnetworking/mad:latest

#### Daemon

The Metrics Aggregator Daemon configuration is specified in a JSON file.  The location of the configuration file is
passed to *mad* as a command line argument:

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
    "jvmMetricsCollectionInterval": "PT1.0S",
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

One instance of Metrics Aggregator daemon supports multiple independent services on the same host.  The most basic
single application host still typically configures two services: i) the end-user application running on the host,
and ii) the system metrics captured by CollectD.  Each of these services is configured as a pipeline in Metrics
Aggregator Daemon.  The pipeline defines the name of the service, one or more sources of metrics and one more
destinations or sinks for the aggregated statistics.

For example:

```json
{
    "name": "MyApplicationPipeline",
    "sources":
    [
        {
            "type": "com.arpnetworking.metrics.common.sources.ClientHttpSourceV1",
            "name": "my_application_http_source"
        },
        {
            "type": "com.arpnetworking.metrics.common.sources.FileSource",
            "name": "my_application_file_source",
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
            "type": "com.arpnetworking.tsdcore.sinks.AggregationServerHttpSink",
            "name": "my_application_aggregation_server_http_sink",
            "uri": "http://192.168.0.2:7066/metrics/v1/data"
        }
    ]
}
```

Each of the pipeline configuration files should be placed in the *pipelinesDirectory* defined as part of the daemon
configuration above.

#### Hocon

The daemon and pipeline configuration files may be written in [Hocon](https://github.com/typesafehub/config) when
specified with a _.conf extension.

### Sources

#### Collectd

Example MAD source configuration:
```json
{
  type="com.arpnetworking.metrics.mad.sources.MappingSource"
  name="collectd_mapping_source"
  "findAndReplace": {
    "^cpu/([\\d]+)/(cpu|percent)/([^/]+)(/value)?": ["cpu/$3", "cpu/by_core/$1/$3"],
    "^snmp/cpu_detailed/([\\d]+)/([^/]+)(/value)?": ["snmp/cpu/$2", "snmp/cpu/by_core/$1/$2"],
    "^load/load/((1|5|15)min)": ["load/$1"],
    "^memory/memory/([^/]+)(/value)?": ["memory/$1"],
    "^vmem/vmpage_number/([^/]+)(/value)?": ["vmem/$1"],
    "^vmem/vmpage_io/([^/]+)/(.*)": ["vmem/io/$1/$2"],
    "^vmem/vmpage_faults/(.*)": ["vmem/faults/$1"],
    "^swap/swap/([^/]+)(/value)?": ["swap/$1"],
    "^swap/swap_io/([^/]+)(/value)?": ["swap/io/$1"],
    "^interface/([^/]+)/if_([^/]+)/(.*)": ["interface/$1/$3/$2"],
    "^disk/([^/]+)/disk_([^/]+)/(read|write)": ["disk/$1/$3/$2"],
    "^df/(.*)(/value)?": ["disk/$1"],
    "^ntpd/(.*)(/value)?": ["ntpd/$1"],
    "^processes/ps_state/([^/]+)(/value)?": ["processes/by_state/$1"],
    "^processes/([^/]+)/ps_(vm|rss|data|code|stacksize)(/value)?": ["processes/by_name/$1/$2"],
    "^processes/([^/]+)/ps_(cputime|count|pagefaults)/(.*)": ["processes/by_name/$1/$2/$3"],
    "^processes/([^/]+)/ps_disk_([^/]+)/(.*)": ["processes/by_name/$1/disk/$3/$2"],
    "^tcpconns/([^-]+)-(local|remote)/tcp_connections/([^/]+)(/value)?": ["tcpconns/$2/$1/$3"],
    "^tcpconns/all/tcp_connections/([^/]+)(/value)?": ["tcpconns/all/$1"],
    "^memcached/df/cache/(.*)": ["memcached/cache/$1"],
    "^memcached/memcached_command/([^/]+)(/value)?": ["memcached/commands/$1"],
    "^memcached/memcached_connections/current(/value)?": ["memcached/connections"],
    "^memcached/memcached_items/current(/value)?": ["memcached/items"],
    "^memcached/memcached_octets/rx": ["memcached/network/bytes_read"],
    "^memcached/memcached_octets/tx": ["memcached/network/bytes_written"],
    "^memcached/memcached_ops/([^/]+)(/value)?": ["memcached/operations/$1"],
    "^memcached/percent/([^/]+)(/value)?": ["memcached/$1"],
    "^memcached/ps_count/.*": [],
    "^memcached/ps_cputime/.*": [],
    "^uptime/uptime(/value)?": ["uptime/value"]
  },
  "source": {
    type="com.arpnetworking.metrics.common.sources.CollectdHttpSourceV1"
    actorName="collectd-http-source"
    name="collectd_http_source"
  }
}
```

Example Collectd write plugin configuration:
```
LoadPlugin write_http
<Plugin "write_http">
  <Node "mad">
    URL "http://localhost:7090/metrics/v1/collectd"
    Format "JSON"
    Header "X-TAG-SERVICE: collectd"
    Header "X-TAG-CLUSTER: collectd_local"
    StoreRates true
    BufferSize 4096
  </Node>
</Plugin>
```

#### Telegraf

Example MAD source configuration:
```json
{
  type="com.arpnetworking.metrics.mad.sources.MappingSource"
  name="telegraftcp_mapping_source"
  findAndReplace={
    "\\."=["/"]
  }
  source={
    type="com.arpnetworking.metrics.common.sources.TcpLineSource"
    actorName="telegraf-tcp-source"
    name="telegraftcp_source"
    host="0.0.0.0"
    port="8094"
    parser={
      type="com.arpnetworking.metrics.mad.parsers.TelegrafJsonToRecordParser"
      timestampUnit="NANOSECONDS"
    }
  }
}
```

Example Telegraf output configuration:
```
[agent]
interval="1s"
flush_interval="1s"
round_interval=true
omit_hostname=false

[global_tags]
service="telegraf"
cluster="telegraf_local"

[[outputs.socket_writer]]
address = "tcp://localhost:8094"
data_format = "json"
json_timestamp_units = "1ns"
keep_alive_period = "5m"

[[inputs.cpu]]
percpu = true
totalcpu = true
collect_cpu_time = false
report_active = false
[[inputs.mem]]
[[inputs.net]]
[[inputs.netstat]]
[[inputs.disk]]
interval="10s"
[[inputs.diskio]]
[[inputs.swap]]
[[inputs.kernel]]
```

### Graphite

Example MAD source configuration:

```json
{
  type="com.arpnetworking.metrics.mad.sources.MappingSource"
  name="graphitetcp_mapping_source"
  actorName="graphite-tcp-source"
  findAndReplace={
    "\\."=["/"]
  }
  source={
    type="com.arpnetworking.metrics.common.sources.TcpLineSource"
    name="graphitetcp_source"
    host="0.0.0.0"
    port="2003"
  }
}
```

Development
-----------

To build the service locally you must satisfy these prerequisites:
* [Docker](http://www.docker.com/) (for [Mac](https://docs.docker.com/docker-for-mac/))

__Note:__ Requires at least Docker for Mac Beta version _Version 1.12.0-rc4-beta19 (build: 10258)_

Next, fork the repository, clone and build.

Unit tests and integration tests can be run from IntelliJ. Integration tests
require that the service and its dependencies be running (see below).

Building:

    metrics-aggregator-daemon> ./jdk-wrapper.sh ./mvnw verify

To launch the service and its dependencies in Docker:

    metrics-aggregator-daemon> ./jdk-wrapper.sh ./mvnw docker:start

To launch the service with remote debugging on port 9001 and its dependencies in Docker:

    metrics-aggregator-daemon> ./jdk-wrapper.sh ./mvnw -Ddebug=true docker:start

To execute performance tests:

    metrics-aggregator-daemon> ./jdk-wrapper.sh ./mvnw -PperformanceTest test

To use the local version in your project you must first install it locally:

    metrics-aggregator-daemon> ./jdk-wrapper.sh ./mvnw install

You can determine the version of the local build from the pom.xml file.  Using the local version is intended only for
testing or development.

You may also need to add the local repository to your build in order to pick-up the local version:

* Maven - Included by default.
* Gradle - Add *mavenLocal()* to *build.gradle* in the *repositories* block.
* SBT - Add *resolvers += Resolver.mavenLocal* into *project/plugins.sbt*.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Groupon Inc., 2014
