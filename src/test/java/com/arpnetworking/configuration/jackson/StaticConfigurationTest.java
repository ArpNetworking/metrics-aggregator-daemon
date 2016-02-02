/**
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
package com.arpnetworking.configuration.jackson;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the <code>StaticConfiguration</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class StaticConfigurationTest {

    @Test
    public void testBuilderSetSources() {
        final StaticConfiguration configuration = new StaticConfiguration.Builder()
                .setSources(Lists.newArrayList(JSON_NODE_SOURCE_A, JSON_NODE_SOURCE_B))
                .build();
        Assert.assertEquals("1", configuration.getRequiredProperty("one"));
        Assert.assertEquals("2", configuration.getRequiredProperty("two"));
    }

    @Test
    public void testBuilderAddSource() {
        final StaticConfiguration configuration = new StaticConfiguration.Builder()
                .addSource(JSON_NODE_SOURCE_A)
                .addSource(JSON_NODE_SOURCE_B)
                .build();
        Assert.assertEquals("1", configuration.getRequiredProperty("one"));
        Assert.assertEquals("2", configuration.getRequiredProperty("two"));
    }

    private static final JsonNodeSource JSON_NODE_SOURCE_A = new JsonNodeLiteralSource.Builder()
            .setSource("{\"one\": \"1\"}")
            .build();
    private static final JsonNodeSource JSON_NODE_SOURCE_B = new JsonNodeLiteralSource.Builder()
            .setSource("{\"two\": \"2\"}")
            .build();
}
