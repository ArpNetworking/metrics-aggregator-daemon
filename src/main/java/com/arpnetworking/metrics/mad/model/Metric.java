/*
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
package com.arpnetworking.metrics.mad.model;

import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

/**
 * Interface for a type of collected data.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public interface Metric {

    /**
     * Accessor for the type of metric.
     *
     * @return The type of metric.
     */
    MetricType getType();

    /**
     * Accessor for the collected sample data.
     *
     * @return The collected sample data.
     */
    List<Quantity> getValues();

    /**
     * Accessor for the collected statistical data.
     *
     * @return The collected statistical data.
     */
    ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> getStatistics();
}
