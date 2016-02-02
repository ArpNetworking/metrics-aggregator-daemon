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
package com.arpnetworking.configuration;

import com.arpnetworking.configuration.triggers.FileTrigger;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;

/**
 * Tests for the <code>FileTrigger</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class FileTriggerTest {

    @BeforeClass
    public static void setUpClass() throws IOException {
        Files.createDirectories(new File("./target/tmp/test/FileTriggerTest").toPath());
    }

    @Test
    public void testInitialFile() throws IOException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testInitialFile");
        Files.deleteIfExists(file.toPath());
        Files.createFile(file.toPath());

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testInitialFileAlwaysExisted() throws IOException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testInitialFileAlwaysExisted");
        Files.deleteIfExists(file.toPath());
        Files.createFile(file.toPath());
        Files.setLastModifiedTime(file.toPath(), FileTime.fromMillis(0L));

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testNoInitialFile() throws IOException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testNoInitialFile");
        Files.deleteIfExists(file.toPath());

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testFileDeleted() throws IOException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testFileDeleted");
        Files.deleteIfExists(file.toPath());
        Files.createFile(file.toPath());

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.deleteIfExists(file.toPath());

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testFileCreated() throws IOException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testFileCreated");
        Files.deleteIfExists(file.toPath());

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.createFile(file.toPath());

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testFileChanged() throws IOException, InterruptedException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testFileChanged");
        Files.deleteIfExists(file.toPath());
        Files.write(file.toPath(), "foo".getBytes(Charsets.UTF_8));

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        // Ensure file system modified time reflects the change
        Thread.sleep(1000);
        Files.write(file.toPath(), "bar".getBytes(Charsets.UTF_8));

        Assert.assertTrue(trigger.evaluateAndReset());
    }

    @Test
    public void testFileChangedLastModifiedOnly() throws IOException {
        final File file = new File("./target/tmp/test/FileTriggerTest/testFileChangedLastModifiedOnly");
        Files.deleteIfExists(file.toPath());
        Files.write(file.toPath(), "foo".getBytes(Charsets.UTF_8));
        Files.setLastModifiedTime(file.toPath(), FileTime.fromMillis(1418112007000L));

        final Trigger trigger = new FileTrigger.Builder()
                .setFile(file)
                .build();

        Assert.assertTrue(trigger.evaluateAndReset());
        Assert.assertFalse(trigger.evaluateAndReset());

        Files.setLastModifiedTime(file.toPath(), FileTime.fromMillis(1529223118111L));

        Assert.assertFalse(trigger.evaluateAndReset());
    }
}
