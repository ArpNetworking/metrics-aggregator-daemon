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
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;

/**
 * Tests for the <code>JsonNodePaginatedUrlSource</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class JsonNodePaginatedUrlSourceTest {

    @Test
    public void test() throws MalformedURLException {
        final WireMockServer server = new WireMockServer(0);
        server.start();
        final WireMock wireMock = new WireMock(server.port());

        wireMock.register(WireMock.get(WireMock.urlEqualTo("/animals?offset=0&limit=1"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json; charset=utf-8")
                        .withBody("{\"metadata\":{\"next\":\"animals?offset=1&limit=1\"},\"data\":[\"cat\"]}")));

        wireMock.register(WireMock.get(WireMock.urlEqualTo("/animals?offset=1&limit=1"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json; charset=utf-8")
                        .withBody("{\"metadata\":{\"next\":\"animals?offset=2&limit=1\"},\"data\":[\"dog\"]}")));

        wireMock.register(WireMock.get(WireMock.urlEqualTo("/animals?offset=2&limit=1"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json; charset=utf-8")
                        .withBody("{\"metadata\":{\"next\":null},\"data\":[\"mouse\"]}")));

        final JsonNodePaginatedUriSource jsonNodePaginatedUrlSource = new JsonNodePaginatedUriSource.Builder()
                .setUri(URI.create("http://localhost:" + server.port() + "/animals?offset=0&limit=1"))
                .setDataKeys(ImmutableList.of("data"))
                .setNextPageKeys(ImmutableList.of("metadata", "next"))
                .build();

        final Optional<JsonNode> jsonNode = jsonNodePaginatedUrlSource.getJsonNode();
        Assert.assertTrue(jsonNode.isPresent());
        Assert.assertTrue(jsonNode.get().isArray());
        Assert.assertEquals(3, ((ArrayNode) jsonNode.get()).size());
        Assert.assertEquals("cat", ((ArrayNode) jsonNode.get()).get(0).textValue());
        Assert.assertEquals("dog", ((ArrayNode) jsonNode.get()).get(1).textValue());
        Assert.assertEquals("mouse", ((ArrayNode) jsonNode.get()).get(2).textValue());

        server.stop();
    }
}
