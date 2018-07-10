/*
 * Copyright 2017 Inscope Metrics, Inc
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
package com.inscopemetrics.mad;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Provides a way to register for lifecycle events, ensuring that Guice-instantiated classes can be notified.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public interface LifecycleRegistration {
    /**
     * Registers a method to be called when the application is shutting down.  The shutdown
     * will wait for all CompletionStages to complete before shutting down.
     *
     * @param callback The callback to register
     */
    void registerShutdown(Supplier<CompletionStage<Void>> callback);
}
