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
package com.arpnetworking.configuration.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Tests for the {@link JsonNodeDirectorySource} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class JsonNodeDirectorySourceTest {

    @BeforeClass
    public static void setUpClass() throws IOException {
        Files.createDirectories(new File("./target/tmp/filter/JsonNodeDirectorySourceTest").toPath());
    }

    @Test
    public void testDirectoryDoesNotExist() {
        final File directory = new File("./target/tmp/filter/JsonNodeDirectorySourceTest/testDirectoryDoesNotExist");
        final JsonNodeDirectorySource source = new JsonNodeDirectorySource.Builder()
                .setDirectory(directory)
                .build();
        Assert.assertFalse(source.getJsonNode().isPresent());
    }

    @Test
    public void testDirectoryNotDirectory() throws IOException {
        final File directory = new File("./target/tmp/filter/JsonNodeDirectorySourceTest/testDirectoryNotDirectory");
        Files.deleteIfExists(directory.toPath());
        Files.createFile(directory.toPath());
        final JsonNodeDirectorySource source = new JsonNodeDirectorySource.Builder()
                .setDirectory(directory)
                .build();
        Assert.assertFalse(source.getJsonNode().isPresent());
    }

    @Test
    public void testDirectoryAll() throws IOException {
        final File directory = new File("./target/tmp/filter/JsonNodeDirectorySourceTest/testDirectoryAll");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());
        Files.write(directory.toPath().resolve("foo.json"), "[\"one\"]".getBytes(StandardCharsets.UTF_8));
        Files.write(directory.toPath().resolve("bar.txt"), "[\"two\"]".getBytes(StandardCharsets.UTF_8));
        final JsonNodeDirectorySource source = new JsonNodeDirectorySource.Builder()
                .setDirectory(directory)
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertTrue(source.getJsonNode().get().isArray());
        final ArrayNode arrayNode = (ArrayNode) source.getJsonNode().get();
        Assert.assertEquals(2, arrayNode.size());
        Assert.assertTrue(arrayNodeContains(arrayNode, "one"));
        Assert.assertTrue(arrayNodeContains(arrayNode, "two"));
    }

    @Test
    public void testDirectoryOnlyMatchingNames() throws IOException {
        final File directory = new File("./target/tmp/filter/JsonNodeDirectorySourceTest/testDirectoryOnlyMatchingNames");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());
        Files.write(directory.toPath().resolve("foo.json"), "[\"one\"]".getBytes(StandardCharsets.UTF_8));
        Files.write(directory.toPath().resolve("bar.txt"), "[\"two\"]".getBytes(StandardCharsets.UTF_8));
        final JsonNodeDirectorySource source = new JsonNodeDirectorySource.Builder()
                .setDirectory(directory)
                .addFileName("foo.json")
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertTrue(source.getJsonNode().get().isArray());
        final ArrayNode arrayNode = (ArrayNode) source.getJsonNode().get();
        Assert.assertEquals(1, arrayNode.size());
        Assert.assertTrue(arrayNodeContains(arrayNode, "one"));
    }

    @Test
    public void testDirectoryOnlyMatchingNamePatterns() throws IOException {
        final File directory = new File("./target/tmp/filter/JsonNodeDirectorySourceTest/testDirectoryOnlyMatchingNamePatterns");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());
        Files.write(directory.toPath().resolve("foo.json"), "[\"one\"]".getBytes(StandardCharsets.UTF_8));
        Files.write(directory.toPath().resolve("bar.txt"), "[\"two\"]".getBytes(StandardCharsets.UTF_8));
        final JsonNodeDirectorySource source = new JsonNodeDirectorySource.Builder()
                .setDirectory(directory)
                .addFileNamePattern(Pattern.compile(".*\\.json"))
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertTrue(source.getJsonNode().get().isArray());
        final ArrayNode arrayNode = (ArrayNode) source.getJsonNode().get();
        Assert.assertEquals(1, arrayNode.size());
        Assert.assertTrue(arrayNodeContains(arrayNode, "one"));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidJson() throws IOException {
        final File directory = new File("./target/tmp/filter/JsonNodeDirectorySourceTest/testInvalidJson.json");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());
        Files.write(directory.toPath().resolve("foo.json"), "this=not-json".getBytes(StandardCharsets.UTF_8));
        Files.write(directory.toPath().resolve("bar.txt"), "\"two\"".getBytes(StandardCharsets.UTF_8));
        new JsonNodeDirectorySource.Builder()
                .setDirectory(directory)
                .build();
    }

    private static boolean arrayNodeContains(final ArrayNode arrayNode, final String value) {
        for (final JsonNode node : arrayNode) {
            if (Objects.equals(node.asText(), value)) {
                return true;
            }
        }
        return false;
    }

    private static void deleteDirectory(final File directory) throws IOException {
        if (directory.exists() && directory.isDirectory()) {
            for (final File file : Optional.ofNullable(directory.listFiles()).orElse(new File[0])) {
                Files.deleteIfExists(file.toPath());
            }
        }
        Files.deleteIfExists(directory.toPath());
    }
}
