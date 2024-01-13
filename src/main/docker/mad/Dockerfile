# Copyright 2016 Inscope Metrics, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:17-slim-bullseye

MAINTAINER arpnetworking

EXPOSE 7090
WORKDIR /opt/mad

# Configuration
ENV MAD_USER="mad"
ENV MAD_UID=1000
ENV MAD_GID=1000
ENV JVM_XMS="64m"
ENV JVM_XMX="1024m"
ENV LOGBACK_CONFIG="-Dlogback.configurationFile=/opt/mad/config/logback-console.xml"
ENV MAD_CONFIG="/opt/mad/config/config.conf"
ENV JAVA_OPTS=""

# Build
RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install -y gosu openssl && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    mkdir -p /opt/mad/lib/ext && \
    mkdir -p /opt/mad/logs && \
    mkdir -p /opt/mad/config/pipelines && \
    mkdir -p /opt/mad/tls && \
    openssl req -x509 -newkey rsa:4096 -keyout /opt/mad/tls/key.pem -out /opt/mad/tls/cert.pem -days 3650 -nodes -addext "subjectAltName = DNS:localhost" -subj '/CN=localhost'
ADD deps /opt/mad/lib/
ADD bin /opt/mad/bin/
ADD config /opt/mad/config/
ADD lib /opt/mad/lib/

# Entry point
ENTRYPOINT [ "/opt/mad/bin/docker-entrypoint.sh" ]

# Default arguments
CMD [ \
    "-n", \
    "${MAD_USER}", \
    "-u", \
    "${MAD_UID}", \
    "-g", \
    "${MAD_GID}", \
    "-d", \
    "/opt/mad", \
    "--", \
    "/opt/mad/bin/mad", \
    "${LOGBACK_CONFIG}", \
    "-XX:+HeapDumpOnOutOfMemoryError", \
    "-XX:HeapDumpPath=/opt/mad/logs/mad.oom.hprof", \
    "-Xlog:gc*:file=/opt/mad/logs/mad.gc.log:time,level,tags:filecount=2,filesize=50M", \
    "-Xms${JVM_XMS}", \
    "-Xmx${JVM_XMX}", \
    "-XX:+UseStringDeduplication", \
    "-XX:+UseG1GC", \
    "-Duser.timezone=UTC", \
    "--", \
    "${MAD_CONFIG}" ]
