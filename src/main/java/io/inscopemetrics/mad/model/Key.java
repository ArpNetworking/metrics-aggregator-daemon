/*
 * Copyright 2016 Ville Koskela
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
package io.inscopemetrics.mad.model;

import com.google.common.collect.ImmutableMap;

/**
 * Interface for aggregation context (e.g. slice of hyper-cube).
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public interface Key {

    /**
     * Return all the slice parameters.
     *
     * @return {@code ImmutableMap} of parameter name to value.
     */
    ImmutableMap<String, String> getParameters();

    /**
     * The cluster parameter value.
     *
     * @return Cluster parameter value.
     */
    String getCluster();

    /**
     * The service parameter value.
     *
     * @return Service parameter value.
     */
    String getService();

    /**
     * The host parameter value.
     *
     * @return Host parameter value.
     */
    String getHost();

    /**
     * The dimension key for the cluster attribute.
     */
    String CLUSTER_DIMENSION_KEY = "cluster";

    /**
     * The dimension key for the service attribute.
     */
    String SERVICE_DIMENSION_KEY = "service";

    /**
     * The dimension key for the host attribute.
     */
    String HOST_DIMENSION_KEY = "host";
}
