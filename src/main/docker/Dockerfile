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

FROM openjdk:8u201-jre-alpine

MAINTAINER inscopemetrics.io

EXPOSE 7090
WORKDIR /opt/mad

# Configuration
#
# There are two groups of arguments:
#   1) JVM (e.g. JVM_XMS, JAVA_OPTS, ADDITIONAL_JAVA_OPTS)
#   2) Application (e.g. MAD_CONFIG, APP_OPTS, ADDITIONAL_APP_OPTS)
#
# Each group has three flavors of arguments:
#   1) Explicit (e.g. JVM_XMS)
#   2) Default (e.g. JAVA_OPTS)
#   3) Additional (e.g. ADDITIONAL_JAVA_OPTS)
#
# The explicit arguments are commonly modified and should be used for these
# parameters instead of relying on default or additional arguments to set them.
#
# The default arguments are less common and rarely need to be modified. When
# you override the default you override all the defaults. You should not use
# defaults to change explicit arguments.
#
# The additional arguments let you add to and in some cases override default
# arguments without specifying all default arguments.
#
# Implementation note: the $JAVA_OPTS setting is added by automatically by the
# run script which is why you don't see it passed explicitly.
#
# Implementation note: when passing arguments with evaluated contents the
# result of the evaluation is not quoted because it cannot be determined
# whether the intention was to result in one or many arguments. Therefore, when
# using evaluated arguments always perform the quoting (if any) yourself.

ENV JVM_XMS="64m"
ENV JVM_XMX="1024m"
ENV LOGBACK_CONFIG="-Dlogback.configurationFile=/opt/mad/config/logback.xml"
ENV MAD_CONFIG="/opt/mad/config/config.conf"
ENV JAVA_OPTS="\
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/opt/mad/logs/mad.oom.hprof \
    -XX:+PrintGCDetails \
    -XX:+PrintGCDateStamps \
    -Xloggc:/opt/mad/logs/mad.gc.log \
    -XX:NumberOfGCLogFiles=2 \
    -XX:GCLogFileSize=50M \
    -XX:+UseGCLogFileRotation \
    -XX:+UseStringDeduplication \
    -XX:+UseG1GC \
    -Duser.timezone=UTC"
ENV ADDITIONAL_JAVA_OPTS=""
ENV APP_OPTS=""
ENV ADDITIONAL_APP_OPTS=""

# Build
RUN mkdir -p /opt/mad/lib/ext && \
    mkdir -p /opt/mad/logs && \
    mkdir -p /opt/mad/config/pipelines
ADD deps /opt/mad/lib/
ADD bin /opt/mad/bin/
ADD config /opt/mad/config/
ADD lib /opt/mad/lib/

# Entry point
ENTRYPOINT [ \
    "/opt/mad/bin/mad", \
    "${LOGBACK_CONFIG}", \
    "-Xms${JVM_XMS}", \
    "-Xmx${JVM_XMX}", \
    "${ADDITIONAL_JAVA_OPTS}", \
    "--", \
    "${APP_OPTS}", \
    "${MAD_CONFIG}", \
    "${ADDITIONAL_APP_OPTS}" ]
