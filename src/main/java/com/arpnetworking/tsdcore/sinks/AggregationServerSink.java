/*
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.PeriodicDataToProtoConverter;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;

/**
 * Publisher to send data to an upstream aggregation server.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class AggregationServerSink extends VertxSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", periodicData.getData().size())
                .log();

        final Collection<Messages.StatisticSetRecord> records = PeriodicDataToProtoConverter.convert(periodicData);

        for (final Messages.StatisticSetRecord record : records) {
            enqueueData(AggregationMessage.create(record).serializeToBuffer());
        }
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .build();
    }

    private void heartbeat() {

        final Messages.HeartbeatRecord message = Messages.HeartbeatRecord.newBuilder()
                .setTimestamp(ZonedDateTime.now().toString())
                .build();
        sendRawData(AggregationMessage.create(message).serializeToBuffer());
        LOGGER.debug()
                .setMessage("Heartbeat sent to aggregation server")
                .addData("sink", getName())
                .log();
    }

    private AggregationServerSink(final Builder builder) {
        super(builder);
        super.getVertx().setPeriodic(HEARTBEAT_INTERVAL_MILLISECONDS, event -> {
            LOGGER.trace()
                    .setMessage("Heartbeat tick")
                    .addData("sink", getName())
                    .log();
            heartbeat();
        });
    }

    private static final int HEARTBEAT_INTERVAL_MILLISECONDS = 15000;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationServerSink.class);

    /**
     * Implementation of builder pattern for ${code AggregationServerSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    public static final class Builder extends VertxSink.Builder<Builder, AggregationServerSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregationServerSink::new);
            setServerPort(DEFAULT_PORT);
        }

        @Override
        protected Builder self() {
            return this;
        }

        private static final int DEFAULT_PORT = 7065;
    }
}
