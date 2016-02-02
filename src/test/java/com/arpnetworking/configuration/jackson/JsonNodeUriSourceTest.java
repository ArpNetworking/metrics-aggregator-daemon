/**
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
package com.arpnetworking.configuration.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;

/**
 * Tests for the <code>JsonNodeUrlSource</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class JsonNodeUriSourceTest {

    @Test
    public void test() throws MalformedURLException {
        final WireMockServer server = new WireMockServer(0);
        server.start();
        final WireMock wireMock = new WireMock(server.port());

        wireMock.register(WireMock.get(WireMock.urlEqualTo("/configuration"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json; charset=utf-8")
                        .withBody("{\"values\":[\"foo\",\"bar\"]}")));

        final JsonNodeUriSource jsonNodeUriSource = new JsonNodeUriSource.Builder()
                .setUri(URI.create("http://localhost:" + server.port() + "/configuration"))
                .build();

        final Optional<JsonNode> jsonNode = jsonNodeUriSource.getJsonNode();
        Assert.assertTrue(jsonNode.isPresent());
        Assert.assertTrue(jsonNode.get().isObject());
        Assert.assertEquals(1, Iterators.size(((ObjectNode) jsonNode.get()).fieldNames()));

        final Optional<JsonNode> valuesJsonNode = jsonNodeUriSource.getValue("values");
        Assert.assertTrue(valuesJsonNode.isPresent());
        Assert.assertTrue(valuesJsonNode.get().isArray());
        Assert.assertEquals(2, ((ArrayNode) valuesJsonNode.get()).size());
        Assert.assertEquals("foo", ((ArrayNode) valuesJsonNode.get()).get(0).textValue());
        Assert.assertEquals("bar", ((ArrayNode) valuesJsonNode.get()).get(1).textValue());

        server.stop();
    }
}
