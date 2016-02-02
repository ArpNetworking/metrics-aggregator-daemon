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

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the <code>JsonNodeMergingSource</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class JsonNodeMergingSourceTest {

    @Test
    public void testEmpty() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder().build();
        Assert.assertFalse(source.getJsonNode().isPresent());
    }

    @Test
    public void testMergeArrayNodeAndArrayNode() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("[\"1\"]").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("[\"2\"]").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        final ArrayNode arrayNode = (ArrayNode) source.getValue().get();
        Assert.assertEquals(2, arrayNode.size());
        Assert.assertEquals("1", arrayNode.get(0).asText());
        Assert.assertEquals("2", arrayNode.get(1).asText());
    }

    @Test
    public void testMergeObjectNodeAndObjectNode() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"1\"}").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"b\":\"2\"}").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("1", source.getValue("a").get().asText());
        Assert.assertEquals("2", source.getValue("b").get().asText());
    }

    @Test
    public void testMergeObjectNodeAndObjectNodeOverwrite() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"1\"}").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"2\"}").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("2", source.getValue("a").get().asText());
    }

    @Test
    public void testMergeReplaceObjectNodeWithLiteral() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"1\"}").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("foo", source.getValue().get().asText());
    }

    @Test
    public void testMergeReplaceArrayNodeWithLiteral() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("[\"1\"]").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("foo", source.getValue().get().asText());
    }

    @Test
    public void testMergeReplaceLiteralWithObjectNode() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"1\"}").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("1", source.getValue("a").get().asText());
    }

    @Test
    public void testMergeReplaceLiteralWithArrayNode() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("[\"1\"]").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        final ArrayNode arrayNode = (ArrayNode) source.getValue().get();
        Assert.assertEquals(1, arrayNode.size());
        Assert.assertEquals("1", arrayNode.get(0).asText());
    }

    @Test
    public void testMergeReplaceArrayNodeWithObjectNode() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("[\"1\"]").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"1\"}").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("1", source.getValue("a").get().asText());
    }

    @Test
    public void testMergeReplaceObjectNodeWithArrayNode() {
        final JsonNodeMergingSource source = new JsonNodeMergingSource.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{\"a\":\"1\"}").build())
                .addSource(new JsonNodeLiteralSource.Builder().setSource("[\"1\"]").build())
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        final ArrayNode arrayNode = (ArrayNode) source.getValue().get();
        Assert.assertEquals(1, arrayNode.size());
        Assert.assertEquals("1", arrayNode.get(0).asText());
    }
}
