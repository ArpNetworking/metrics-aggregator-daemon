/*
 * Copyright 2020 Dropbox
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
package com.arpnetworking.http;

import akka.http.javadsl.model.HttpRequest;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.utility.BaseActorTest;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link Routes} class.
 *
 * @author Qinyan Li (lqy520s at hotmail dot com)
 */
public class RoutesTest extends BaseActorTest {

    @Before
    @Override
    public void startup() {
        super.startup();
        final ImmutableList.Builder<SupplementalRoutes> supplementalHttpRoutes = ImmutableList.builder();
        _routes = new com.arpnetworking.http.Routes(
                getSystem(),
                _metrics,
                HEALTH_CHECK_PATH,
                STATUS_PATH,
                supplementalHttpRoutes.build());
    }

    @Test
    public void testApplyKnownRequest() throws InterruptedException {
        final HttpRequest request = HttpRequest.create("test_health_check");
        _routes.apply(request);

        // wait for the request to complete. The actor time out is 1 second so here wait for 2 seconds.
        Thread.sleep(2000);

        Mockito.verify(_metrics).recordTimer(
                Mockito.eq("rest_service/GET/test_health_check/request"),
                Mockito.anyLong(),
                Mockito.eq(Optional.of(TimeUnit.NANOSECONDS)));
        Mockito.verify(_metrics).recordGauge(
                Mockito.eq("rest_service/GET/test_health_check/body_size"),
                Mockito.anyLong());
        Mockito.verify(_metrics).recordCounter("rest_service/GET/test_health_check/status/5xx", 1);
    }

    @Test
    public void testApplyUnknownRequest() throws InterruptedException {
        final HttpRequest unknownRequest = HttpRequest.create("kb23k1dnlkns02");
        _routes.apply(unknownRequest);

        // wait for the request to complete. Since unknown_route will not call actor, don't have to wait for 2 seconds.
        Thread.sleep(1000);

        Mockito.verify(_metrics).recordTimer(
                Mockito.eq("rest_service/GET/unknown_route/request"),
                Mockito.anyLong(),
                Mockito.eq(Optional.of(TimeUnit.NANOSECONDS)));
        Mockito.verify(_metrics).recordGauge(
                Mockito.eq("rest_service/GET/unknown_route/body_size"),
                Mockito.anyLong());
        Mockito.verify(_metrics).recordCounter("rest_service/GET/unknown_route/status/4xx", 1);
    }

    private Routes _routes;

    private static final String HEALTH_CHECK_PATH = "test_health_check";
    private static final String STATUS_PATH = "test_status";

    @Mock
    private PeriodicMetrics _metrics;
}
