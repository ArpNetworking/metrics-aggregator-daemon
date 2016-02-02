/**
 * Copyright 2015 Groupon.com
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
package com.arpnetworking.utility;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import org.joda.time.Duration;

/**
 * A {@link Trigger} that waits a set amount of time then fires.
 *
 * @author Brandon Arp (barp at groupon dot com)
 */
public class TimerTrigger implements Trigger {

    /**
     * Public constructor.
     *
     * @param duration Wait time
     */
    public TimerTrigger(final Duration duration) {
        _duration = duration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitOnTrigger() throws InterruptedException {
        Thread.sleep(_duration.getMillis());
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("duration", _duration)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private final Duration _duration;
}
