/*
 * Copyright 2018 Dropbox Inc.
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
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.PeriodicDataToProtoConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;

import java.util.Collection;
import java.util.Map;

/**
 * Publisher to send data to an upstream aggregation server over HTTP.
 *
 * @author Ville Koskela (vkoskela at dropbox dot com)
 */
public final class AggregationServerHttpSink extends HttpPostSink {

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .build();
    }

    @Override
    protected String getContentType() {
        return MediaType.PROTOBUF.toString();
    }

    @Override
    protected Collection<byte[]> serialize(final PeriodicData periodicData) {
        final ImmutableList.Builder<byte[]> serializedPeriodicData = ImmutableList.builder();
        final PeriodicDataToProtoConverter converter = new PeriodicDataToProtoConverter(periodicData);
        for (final Map.Entry<String, Collection<AggregatedData>> entry : periodicData.getData().asMap().entrySet()) {
            final String metricName = entry.getKey();
            final Collection<AggregatedData> data = entry.getValue();
            if (!data.isEmpty()) {
                final Messages.StatisticSetRecord record = converter.serializeMetricData(metricName, data);
                serializedPeriodicData.add(AggregationMessage.create(record).serializeToByteString().toArray());
            }
        }
        return serializedPeriodicData.build();
    }

    private AggregationServerHttpSink(final Builder builder) {
        super(builder);
    }

    /**
     * Implementation of builder pattern for ${code AggregationServerHttpSink}.
     *
     * @author Ville Koskela (ville dot koskela at dropbox dot com)
     */
    public static final class Builder extends HttpPostSink.Builder<Builder, AggregationServerHttpSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregationServerHttpSink::new);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
