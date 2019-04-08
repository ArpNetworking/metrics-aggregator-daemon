/*
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
package io.inscopemetrics.mad.utility;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;

import java.time.Duration;

/**
 * A {@link Trigger} that waits a set amount of time then fires.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
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

    @Override
    public void waitOnReadTrigger() throws InterruptedException {
        Thread.sleep(_duration.toMillis());
    }

    @Override
    public void waitOnFileNotFoundTrigger(final int attempt) throws InterruptedException {
        // Max time = 1.3^n * base  (n capped at 20)
        final double maxBackoff = Math.pow(1.3, Math.min(attempt, 20)) * _duration.toMillis();
        // Sleep duration is random from 0 to max, capped at 30 seconds
        final int sleepDurationMillis = (int) Math.max(Math.random() * maxBackoff, 30000);
        Thread.sleep(sleepDurationMillis);
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

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private final Duration _duration;
}
