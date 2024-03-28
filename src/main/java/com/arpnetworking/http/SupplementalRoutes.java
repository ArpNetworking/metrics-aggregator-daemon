/*
 * Copyright 2016 Inscope Metrics Inc.
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

import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.japi.function.Function;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Supplemental http server routes.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public interface SupplementalRoutes extends Function<HttpRequest, Optional<CompletionStage<HttpResponse>>> {

    // Intentionally empty.
}
