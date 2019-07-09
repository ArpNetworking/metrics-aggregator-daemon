/*
 * Copyright 2019 Dropbox.com
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
package com.arpnetworking.metrics.common.kafka;

/**
 * Interface for classes which use a kafka {@code Consumer} to
 * continually poll a kafka topic.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public interface RunnableConsumer extends Runnable {

    /**
     * Stop the {@link RunnableConsumer} instance.
     */
    void stop();
}
