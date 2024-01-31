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

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.http.SupplementalRoutes;
import com.arpnetworking.logback.annotations.Loggable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Representation of TsdAggregator configuration.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@Loggable
public final class AggregatorConfiguration {

    public String getMonitoringCluster() {
        return _monitoringCluster;
    }

    public String getMonitoringService() {
        return _monitoringService;
    }

    public Optional<String> getMonitoringHost() {
        return _monitoringHost;
    }

    public ImmutableList<JsonNode> getMonitoringSinks() {
        return _monitoringSinks;
    }

    @Deprecated
    public Optional<String> getMetricsClientHost() {
        return _metricsClientHost;
    }

    @Deprecated
    public Optional<Integer> getMetricsClientPort() {
        return _metricsClientPort;
    }

    public Duration getJvmMetricsCollectionInterval() {
        return _jvmMetricsCollectionInterval;
    }

    public File getLogDirectory() {
        return _logDirectory;
    }

    public File getPipelinesDirectory() {
        return _pipelinesDirectory;
    }

    public String getHttpHost() {
        return _httpHost;
    }

    public int getHttpPort() {
        return _httpPort;
    }

    public String getHttpsHost() {
        return _httpsHost;
    }

    public int getHttpsPort() {
        return _httpsPort;
    }

    public String getHttpsKeyPath() {
        return _httpsKeyPath;
    }

    public String getHttpsCertificatePath() {
        return _httpsCertificatePath;
    }

    public boolean getEnableHttps() {
        return _enableHttps;
    }

    public String getHttpHealthCheckPath() {
        return _httpHealthCheckPath;
    }

    public String getHttpStatusPath() {
        return _httpStatusPath;
    }

    public Optional<Class<? extends SupplementalRoutes>> getSupplementalHttpRoutesClass() {
        return _supplementalHttpRoutesClass;
    }

    public boolean getLogDeadLetters() {
        return _logDeadLetters;
    }

    public Map<String, ?> getPekkoConfiguration() {
        return Collections.unmodifiableMap(_pekkoConfiguration);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("MonitoringCluster", _monitoringCluster)
                .add("MonitoringService", _monitoringService)
                .add("MonitoringHost", _monitoringHost)
                .add("MonitoringSinks", _monitoringSinks)
                .add("MetricsClientHost", _metricsClientHost)
                .add("MetricsClientPort", _metricsClientPort)
                .add("JvmMetricsCollectorInterval", _jvmMetricsCollectionInterval)
                .add("LogDirectory", _logDirectory)
                .add("PipelinesDirectory", _pipelinesDirectory)
                .add("HttpHost", _httpHost)
                .add("HttpPort", _httpPort)
                .add("HttpsHost", _httpsHost)
                .add("HttpsPort", _httpsPort)
                .add("HttpsKeyPath", _httpsKeyPath)
                .add("HttpsCertificatePath", _httpsCertificatePath)
                .add("EnableHttps", _enableHttps)
                .add("HttpHealthCheckPath", _httpHealthCheckPath)
                .add("HttpStatusPath", _httpStatusPath)
                .add("SupplementalHttpRoutesClass", _supplementalHttpRoutesClass)
                .add("LogDeadLetters", _logDeadLetters)
                .add("PekkoConfiguration", _pekkoConfiguration)
                .toString();
    }

    private AggregatorConfiguration(final Builder builder) {
        _monitoringCluster = builder._monitoringCluster;
        _monitoringService = builder._monitoringService;
        _monitoringSinks = builder._monitoringSinks;
        _jvmMetricsCollectionInterval = builder._jvmMetricsCollectionInterval;
        _logDirectory = builder._logDirectory;
        _pipelinesDirectory = builder._pipelinesDirectory;
        _httpHost = builder._httpHost;
        _httpPort = builder._httpPort;
        _httpsHost = builder._httpsHost;
        _httpsPort = builder._httpsPort;
        _httpsKeyPath = builder._httpsKeyPath;
        _httpsCertificatePath = builder._httpsCertificatePath;
        _enableHttps = builder._enableHttps;
        _httpHealthCheckPath = builder._httpHealthCheckPath;
        _httpStatusPath = builder._httpStatusPath;
        _supplementalHttpRoutesClass = Optional.ofNullable(builder._supplementalHttpRoutesClass);
        _logDeadLetters = builder._logDeadLetters;
        _pekkoConfiguration = builder._pekkoConfiguration;
        _monitoringHost = Optional.ofNullable(builder._monitoringHost);

        // Deprecated legacy settings
        _metricsClientHost = Optional.ofNullable(builder._metricsClientHost);
        _metricsClientPort = Optional.ofNullable(builder._metricsClientPort);
    }

    private final String _monitoringCluster;
    private final String _monitoringService;
    private final Optional<String> _monitoringHost;
    private final ImmutableList<JsonNode> _monitoringSinks;
    private final Optional<String> _metricsClientHost;
    private final Optional<Integer> _metricsClientPort;
    private final Duration _jvmMetricsCollectionInterval;
    private final File _logDirectory;
    private final File _pipelinesDirectory;
    private final String _httpHost;
    private final String _httpsHost;
    private final String _httpHealthCheckPath;
    private final String _httpStatusPath;
    private final int _httpPort;
    private final int _httpsPort;
    private final Optional<Class<? extends SupplementalRoutes>> _supplementalHttpRoutesClass;
    private final boolean _logDeadLetters;
    private final boolean _enableHttps;
    private final String _httpsKeyPath;
    private final String _httpsCertificatePath;
    private final Map<String, ?> _pekkoConfiguration;

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link AggregatorConfiguration}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static class Builder extends OvalBuilder<AggregatorConfiguration> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregatorConfiguration::new);

            final ObjectNode sinkRoot = OBJECT_MAPPER.createObjectNode();
            sinkRoot.set("class", new TextNode("com.arpnetworking.metrics.impl.ApacheHttpSink"));
            _monitoringSinks = ImmutableList.of(sinkRoot);
        }

        /**
         * The monitoring cluster. Cannot be null or empty.
         *
         * @param value The monitoring cluster.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringCluster(final String value) {
            _monitoringCluster = value;
            return this;
        }

        /**
         * The monitoring service. Optional. Cannot be null or empty. The
         * default value is {@code mad}.
         *
         * @param value The monitoring service.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringService(final String value) {
            _monitoringService = value;
            return this;
        }

        /**
         * The monitoring sinks. Optional. Cannot be null. The default value is
         * the default instance of {@link com.arpnetworking.metrics.impl.ApacheHttpSink}.
         *
         * @param value The monitoring sinks.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringSinks(final ImmutableList<JsonNode> value) {
            _monitoringSinks = value;
            return this;
        }

        /**
         * The metrics client http host address to send to. Optional. Cannot be
         * empty. Defaults to unspecified.
         *
         * @param value The metrics client host address to send to.
         * @return This instance of {@link Builder}.
         * @deprecated Use {@link Builder#setMonitoringSinks(ImmutableList)}
         */
        @Deprecated
        public Builder setMetricsClientHost(@Nullable final String value) {
            _metricsClientHost = value;
            return this;
        }

        /**
         * The metrics client http port to send to. Optional. Must be between
         * 1 and 65535 (inclusive). Defaults to unspecified.
         *
         * @param value The metrics client port to listen send to.
         * @return This instance of {@link Builder}.
         * @deprecated Use {@link Builder#setMonitoringSinks(ImmutableList)}
         */
        @Deprecated
        public Builder setMetricsClientPort(@Nullable final Integer value) {
            _metricsClientPort = value;
            return this;
        }

        /**
         * Period for collecting JVM metrics. Optional. Default is 500 milliseconds.
         *
         * @param value A {@link Duration} value.
         * @return This instance of {@link Builder}.
         */
        public Builder setJvmMetricsCollectionInterval(final Duration value) {
            _jvmMetricsCollectionInterval = value;
            return this;
        }

        /**
         * The log directory. Cannot be null.
         *
         * @param value The log directory.
         * @return This instance of {@link Builder}.
         */
        public Builder setLogDirectory(final File value) {
            _logDirectory = value;
            return this;
        }

        /**
         * The pipelines directory. Cannot be null.
         *
         * @param value The pipelines directory.
         * @return This instance of {@link Builder}.
         */
        public Builder setPipelinesDirectory(final File value) {
            _pipelinesDirectory = value;
            return this;
        }

        /**
         * The http host address to bind to. Cannot be null or empty.
         *
         * @param value The host address to bind to.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpHost(final String value) {
            _httpHost = value;
            return this;
        }

        /**
         * The https host address to bind to. Cannot be null or empty.
         *
         * @param value The host address to bind to.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpsHost(final String value) {
            _httpsHost = value;
            return this;
        }

        /**
         * The http health check path. Cannot be null or empty. Optional. Default is "/ping".
         *
         * @param value The health check path.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpHealthCheckPath(final String value) {
            _httpHealthCheckPath = value;
            return this;
        }

        /**
         * The http status path. Cannot be null or empty. Optional. Default is "/status".
         *
         * @param value The status path.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpStatusPath(final String value) {
            _httpStatusPath = value;
            return this;
        }

        /**
         * The location of the https key file. Cannot be null or empty. Optional. Default is "/opt/mad/tls/key.pem".
         *
         * @param value The path to the https key file
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpsKeyPath(final String value) {
            _httpsKeyPath = value;
            return this;
        }

        /**
         * The location of the https certificate file. Cannot be null or empty. Optional. Default is "/opt/mad/tls/cert.pem".
         *
         * @param value The path to the https cert file
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpsCertificatePath(final String value) {
            _httpsCertificatePath = value;
            return this;
        }

        /**
         * The http port to listen on. Cannot be null, must be between 1 and
         * 65535 (inclusive).
         *
         * @param value The port to listen on.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpPort(final Integer value) {
            _httpPort = value;
            return this;
        }

        /**
         * The https port to listen on. Cannot be null, must be between 1 and
         * 65535 (inclusive).
         *
         * @param value The port to listen on.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpsPort(final Integer value) {
            _httpsPort = value;
            return this;
        }

        /**
         * Whether to start the https server. Value cannot be null. Defaults to {@code false}
         *
         * @param value {@code True} if the https server should be enabled
         * @return This instance of {@link Builder}.
         */
        public Builder setEnableHttps(final Boolean value) {
            _enableHttps = value;
            return this;
        }

        /**
         * The supplemental routes class. Optional.
         *
         * @param value The class of the supplement routes.
         * @return This instance of {@link Builder}.
         */
        public Builder setSupplementalHttpRoutesClass(final Class<? extends SupplementalRoutes> value) {
            _supplementalHttpRoutesClass = value;
            return this;
        }

        /**
         * The host to use as value for the host tag. Optional. Defaults to looking up the hostname.
         *
         * @param value The host to use as value for the host tag.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringHost(final String value) {
            _monitoringHost = value;
            return this;
        }

        /**
         * Whether to install the {@link com.arpnetworking.metrics.mad.actors.DeadLetterLogger}
         * to log all dead letter senders, recipients and messages. It differs
         * from the built-in Pekko logging in that it actually logs the message
         * (as provided by {@code toString()}) instead of just the message type.
         *
         * @param value {@code True} if dead letter logging should be enabled
         * @return This instance of {@link Builder}.
         */
        public Builder setLogDeadLetters(final Boolean value) {
            _logDeadLetters = value;
            return this;
        }

        /**
         * Pekko configuration. Cannot be null. By convention Pekko configuration
         * begins with a map containing a single key "pekko" and a value of a
         * nested map. For more information please see:
         *
         * https://pekko.apache.org/docs/pekko/current/general/configuration.html
         *
         * NOTE: No validation is performed on the Pekko configuration itself.
         *
         * @param value The Pekko configuration.
         * @return This instance of {@link Builder}.
         */
        public Builder setPekkoConfiguration(final Map<String, ?> value) {
            _pekkoConfiguration = value;
            return this;
        }

        @NotNull
        @NotEmpty
        private String _monitoringCluster;
        @NotNull
        @NotEmpty
        private String _monitoringService = "mad";
        @Nullable
        @NotEmpty
        private String _monitoringHost;
        // TODO(ville): Apply the default here once we migrate off JsonNode.
        @NotNull
        private ImmutableList<JsonNode> _monitoringSinks;
        @Nullable
        @NotEmpty
        private String _metricsClientHost;
        @Nullable
        @Range(min = 1, max = 65535)
        private Integer _metricsClientPort;
        @NotNull
        private Duration _jvmMetricsCollectionInterval = Duration.ofMillis(1000);
        @NotNull
        private File _logDirectory;
        @NotNull
        private File _pipelinesDirectory;
        @NotNull
        @NotEmpty
        private String _httpHost;
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _httpPort;
        @NotNull
        @NotEmpty
        private String _httpsHost;
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _httpsPort;
        @NotNull
        @NotEmpty
        private String _httpsKeyPath = "/opt/mad/tls/key.pem";
        @NotNull
        @NotEmpty
        private String _httpsCertificatePath = "/opt/mad/tls/cert.pem";
        @NotNull
        private Boolean _enableHttps = false;
        @NotNull
        @NotEmpty
        private String _httpHealthCheckPath = "/ping";
        @NotNull
        @NotEmpty
        private String _httpStatusPath = "/status";
        private Class<? extends SupplementalRoutes> _supplementalHttpRoutesClass;
        @NotNull
        private Boolean _logDeadLetters = false;
        @NotNull
        private Map<String, ?> _pekkoConfiguration;
    }
}
