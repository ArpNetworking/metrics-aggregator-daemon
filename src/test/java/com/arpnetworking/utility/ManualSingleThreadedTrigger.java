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

package com.arpnetworking.utility;


import com.google.common.base.MoreObjects;

import java.util.concurrent.Semaphore;

/**
 * Implementation of {@link com.arpnetworking.utility.Trigger} relying on manual firing (from a different thread) to
 * be released.
 *
 * NOTE: this class is only intended to coordinate interaction between one waiter and one trigger thread for testing.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ManualSingleThreadedTrigger implements Trigger {
    @Override
    public void waitOnReadTrigger() throws InterruptedException {
        if (_enabled) {
            _waiter.release();
            _semaphore.acquire();
        }
    }

    @Override
    public void waitOnFileNotFoundTrigger(final int attempt) throws InterruptedException {
        if (_enabled) {
            _waiter.release();
            _semaphore.acquire();
        }
    }

    /**
     * Allows a single waiting thread to continue.
     */
    public void releaseTrigger() {
        _semaphore.release();
    }

    /**
     * Waits for another thread to wait on the trigger.
     *
     * @throws InterruptedException thrown when the thread is interrupted during the wait
     */
    public void waitForWait() throws InterruptedException {
        _waiter.acquire();
    }

    /**
     * Disables waits on the trigger.
     */
    public void disable() {
        _enabled = false;
        releaseTrigger();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Enabled", _enabled)
                .add("Semaphore", _semaphore)
                .add("Waiter", _waiter)
                .toString();
    }

    private volatile boolean _enabled = true;
    private final Semaphore _semaphore = new Semaphore(0);
    private final Semaphore _waiter = new Semaphore(0);
}
