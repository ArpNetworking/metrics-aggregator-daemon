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
package com.arpnetworking.metrics.mad.model;

import com.google.common.collect.ImmutableMap;

import java.time.ZonedDateTime;

/**
 * The interface to a record. Records consistent of a timestamp, any number of
 * named metrics, annotations (arbitrary key-value pairs) and dimensions (arbitrary key-value pairs).
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 * @author Ryan Ascheman (rascheman at groupon dot com)
 */
public interface Record {

    /**
     * Gets the unique identifier of the record.
     *
     * @return the identifier.
     */
    String getId();

    /**
     * Gets the time stamp of the record.
     *
     * @return the time stamp.
     */
    ZonedDateTime getTime();

    /**
     * Gets the "received at" time stamp of the record.
     *
     * @return the time stamp
     */
    ZonedDateTime getRequestTime();

    /**
     * Gets metrics.
     *
     * @return the metrics
     */
    ImmutableMap<String, ? extends Metric> getMetrics();

    /**
     * Gets annotations.
     *
     * @return the annotations
     */
    ImmutableMap<String, String> getAnnotations();

    /**
     * Gets dimensions.
     *
     * @return the dimensions
     */
    ImmutableMap<String, String> getDimensions();
}
