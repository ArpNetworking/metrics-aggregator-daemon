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

FROM openjdk:8u111-jre-alpine

MAINTAINER arpnetworking

EXPOSE 7090
WORKDIR /opt/mad

# Configuration
ENV JVM_XMS="64m"
ENV JVM_XMX="1024m"
ENV LOGBACK_CONFIG="-Dlogback.configurationFile=/opt/mad/config/logback.xml"
ENV APP_PARAMS="/opt/mad/config/config.conf"
ENV ADDITIONAL_APP_PARAMS=""
ENV ADDITIONAL_JAVA_OPTS=""

# Build
RUN mkdir -p /opt/mad/lib/ext && \
    mkdir -p /opt/mad/logs && \
    mkdir -p /opt/mad/config/pipelines
ADD deps /opt/mad/lib/
ADD bin /opt/mad/bin/
ADD config /opt/mad/config/
ADD lib /opt/mad/lib/

# Entry point
CMD JAVA_OPTS="${LOGBACK_CONFIG} \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/opt/mad/logs/mad.oom.hprof \
    -XX:+PrintGCDetails \
    -XX:+PrintGCDateStamps \
    -Xloggc:logs/mad.gc.log \
    -XX:NumberOfGCLogFiles=2 \
    -XX:GCLogFileSize=50M \
    -XX:+UseGCLogFileRotation \
    -Xms${JVM_XMS} \
    -Xmx${JVM_XMX} \
    -XX:+UseStringDeduplication \
    -XX:+UseG1GC \
    -Duser.timezone=UTC \
    ${ADDITIONAL_JAVA_OPTS}" \
    /opt/mad/bin/mad ${APP_PARAMS} ${ADDITIONAL_APP_PARAMS}
