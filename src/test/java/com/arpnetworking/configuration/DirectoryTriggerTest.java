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
package com.arpnetworking.configuration;

import com.arpnetworking.configuration.triggers.DirectoryTrigger;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Tests for the ${link DirectoryTrigger} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class DirectoryTriggerTest {

    @BeforeClass
    public static void setUpClass() throws IOException {
        Files.createDirectories(new File("./target/tmp/filter/DirectoryTriggerTest").toPath());
    }

    @Test
    public void testInitialDirectory() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testInitialDirectory");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testNoInitialDirectory() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testNoInitialDirectory");
        deleteDirectory(directory);

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testDirectoryDeleted() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testDirectoryDeleted");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        deleteDirectory(directory);

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testDirectoryCreated() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testDirectoryCreated");
        deleteDirectory(directory);

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.createDirectory(directory.toPath());

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testDirectoryChangedFileCreated() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testDirectoryChangedFileCreated");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.write(directory.toPath().resolve("foo.txt"), "bar".getBytes(Charsets.UTF_8));

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testDirectoryChangedFileModified() throws IOException, InterruptedException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testDirectoryChangedFileModified");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());
        final File file = directory.toPath().resolve("foo.txt").toFile();
        Files.createFile(file.toPath());

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        // Ensure file system modified time reflects the change
        Thread.sleep(1000);
        Files.write(file.toPath(), "bar".getBytes(Charsets.UTF_8));

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testDirectoryChangedFileDeleted() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testDirectoryChangedFileDeleted");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());
        final File file = directory.toPath().resolve("foo.txt").toFile();
        Files.write(file.toPath(), "bar".getBytes(Charsets.UTF_8));

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.deleteIfExists(file.toPath());

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testOnlyMatchedName() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testIgnoreUnmatchedName");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .addFileName("bar.txt")
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.write(directory.toPath().resolve("foo.txt"), "bar".getBytes(Charsets.UTF_8));
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.write(directory.toPath().resolve("bar.txt"), "bar".getBytes(Charsets.UTF_8));
        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testOnlyMatchedNamePattern() throws IOException {
        final File directory = new File("./target/tmp/filter/DirectoryTriggerTest/testIgnoreUnmatchedNamePattern");
        deleteDirectory(directory);
        Files.createDirectory(directory.toPath());

        final Trigger trigger = new DirectoryTrigger.Builder()
                .setDirectory(directory)
                .addFileNamePattern(Pattern.compile(".*\\.json"))
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.write(directory.toPath().resolve("foo.txt"), "bar".getBytes(Charsets.UTF_8));
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.write(directory.toPath().resolve("foo.json"), "bar".getBytes(Charsets.UTF_8));
        Assert.assertTrue(trigger.evaluateAndReset());
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
