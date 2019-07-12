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

/**
 * A simple interface that blocks the current thread.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public interface Trigger {
    /**
     * Blocks the current thread.
     *
     * @throws InterruptedException thrown when the wait is interrupted.
     */
    void waitOnReadTrigger() throws InterruptedException;

    /**
     * Blocks the current thread.
     *
     * @param attempt The attempt number to open or find the file. Used for exponential backoff.
     * @throws InterruptedException thrown when the wait is interrupted.
     */
    void waitOnFileNotFoundTrigger(int attempt) throws InterruptedException;
}
