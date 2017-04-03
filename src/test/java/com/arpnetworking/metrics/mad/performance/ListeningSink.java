/**
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
package com.arpnetworking.metrics.mad.performance;

import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.google.common.base.Function;

/**
 * Test helper to provide a callback for a sink.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ListeningSink implements Sink {
    /**
     * Public constructor.
     *
     * @param callback The callback function to execute.
     */
    public ListeningSink(final Function<PeriodicData, Void> callback) {
        _callback = callback;
    }

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        if (_callback != null) {
            _callback.apply(periodicData);
        }
    }

    @Override
    public void close() {
    }

    private final Function<PeriodicData, Void> _callback;
}
