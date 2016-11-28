/**
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
package com.arpnetworking.configuration.jackson;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;

/**
 * Tests for the <code>HoconFileSource</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class HoconFileSourceTest {

    @BeforeClass
    public static void setUpClass() throws IOException {
        Files.createDirectories(new File("./target/tmp/filter/HoconFileSourceTest").toPath());
    }

    @Test
    public void testFileDoesNotExist() throws IOException {
        final File file = new File("./target/tmp/filter/HoconFileSourceTest/testFileDoesNotExist.json");
        final HoconFileSource source = new HoconFileSource.Builder()
                .setFile(file)
                .build();
        Assert.assertFalse(source.getJsonNode().isPresent());
    }

    @Test
    public void testFileUnreadable() throws IOException {
        final File file = new File("./target/tmp/filter/HoconFileSourceTest/testFileUnreadable.json");
        Files.write(file.toPath(), "foo=\"bar\"".getBytes(Charsets.UTF_8));
        Files.setPosixFilePermissions(file.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_WRITE));
        final HoconFileSource source = new HoconFileSource.Builder()
                .setFile(file)
                .build();
        Assert.assertFalse(source.getJsonNode().isPresent());
    }

    @Test
    public void testValidHocon() throws IOException {
        final File file = new File("./target/tmp/filter/HoconFileSourceTest/testValidHocon.hocon");
        Files.write(file.toPath(), "foo:\"bar\"".getBytes(Charsets.UTF_8));
        final HoconFileSource source = new HoconFileSource.Builder()
                .setFile(file)
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("bar", source.getValue("foo").get().textValue());
        Assert.assertFalse(source.getValue("does-not-exist").isPresent());
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidHocon() throws IOException {
        final File file = new File("./target/tmp/filter/HoconFileSourceTest/testInvalidHocon.json");
        Files.write(file.toPath(), "This=\"not-hocon".getBytes(Charsets.UTF_8));
        new HoconFileSource.Builder()
                .setFile(file)
                .build();
    }
}
