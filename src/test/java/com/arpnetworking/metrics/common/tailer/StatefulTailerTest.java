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

package com.arpnetworking.metrics.common.tailer;

import com.arpnetworking.utility.ManualSingleThreadedTrigger;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the <code>StatefulTailer</code> class.
 *
 * TODO(vkoskela): Add tests for interacting with position store. [MAI-321]
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class StatefulTailerTest {

    @Before
    public void setUp() throws IOException {
        _directory = Files.createDirectories(Paths.get("./target/tmp/filter/StatefulTailerTest"));
        _file = Files.createTempFile(_directory, "", "");
        Files.deleteIfExists(_file);

        Mockito.when(_positionStore.getPosition(Mockito.anyString())).thenReturn(Optional.<Long>empty());
        _readTrigger = new ManualSingleThreadedTrigger();

        final StatefulTailer.Builder builder = new StatefulTailer.Builder()
                .setListener(_listener)
                .setFile(_file)
                .setPositionStore(_positionStore)
                .setReadInterval(READ_INTERVAL);
        _tailer = new StatefulTailer(builder, _readTrigger);
    }

    @Test
    public void testReadData() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 10, expectedValues);
        }

        _executor.execute(_tailer);
        _readTrigger.waitForWait();
        _readTrigger.disable();
        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener, Mockito.never()).fileRotated();
        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testReadDataAfterFileCreation() throws IOException, InterruptedException {

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 10, expectedValues);
        }

        _readTrigger.releaseTrigger();
        // Make sure we read the file
        _readTrigger.waitForWait();
        _readTrigger.disable();
        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).fileRotated();
        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener, Mockito.atLeastOnce()).fileNotFound();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testReadDataDifferentLineTerminators() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            for (int i = 0; i < 12; ++i) {
                final String value = UUID.randomUUID().toString();
                expectedValues.add(value);
                if (i % 3 == 0) {
                    writer.write(value + "\n");
                } else if (i % 3 == 1) {
                    writer.write(value + "\r");
                } else {
                    writer.write(value + "\r\n");
                }
            }
        }

        _executor.execute(_tailer);
        _readTrigger.waitForWait();
        _readTrigger.disable();
        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener, Mockito.never()).fileRotated();
        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testTailData() throws IOException, InterruptedException {
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            _executor.execute(_tailer);
            Mockito.verify(_listener).initialize(_tailer);

            // Offset the sleep schedules (this is very brittle)
            _readTrigger.waitForWait();

            for (int i = 0; i < 10; ++i) {
                final String value = UUID.randomUUID().toString();
                writer.write(value + "\n");
                writer.flush();
                _readTrigger.releaseTrigger();
                _readTrigger.waitForWait();
                Mockito.verify(_listener).handle(value.getBytes(Charsets.UTF_8));
            }

            _readTrigger.disable();
        }
        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener, Mockito.never()).fileRotated();
        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
    }

    @Test
    public void testReadDataWithZeroOffset() throws IOException, InterruptedException {
        Mockito.when(_positionStore.getPosition(Mockito.anyString())).thenReturn(Optional.of(0L));

        final BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW);

        final List<String> expectedValues = Lists.newArrayList();
        writeUuids(writer, 5, expectedValues);
        writer.flush();

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        writeUuids(writer, 5, expectedValues);
        writer.close();

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testReadDataWithNonZeroOffsetInSmallFile() throws IOException, InterruptedException {
        final BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW);

        // Insufficient data for hash; 5 * (36 + 1) = 185 bytes; 20 UUIDs of 36 characters plus line break
        final List<String> expectedValues = Lists.newArrayList();
        writeUuids(writer, 5, expectedValues);
        writer.flush();

        _executor.execute(_tailer);

        _readTrigger.waitForWait();

        writeUuids(writer, 5, expectedValues);
        writer.close();

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileOpened();
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
        Mockito.verifyNoMoreInteractions(_listener);
    }

    @Test
    public void testReadDataWithNonZeroOffsetInLargeFile() throws IOException, InterruptedException {
        // Sufficient data for hash; 15 * (36 + 1) = 555 bytes; 15 UUIDs of 36 characters plus line break
        Mockito.when(_positionStore.getPosition(Mockito.anyString())).thenReturn(Optional.of(555L));
        Mockito.doNothing().when(_positionStore).setPosition(Mockito.anyString(), Mockito.anyLong());

        final BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW);

        final List<String> expectedValues = Lists.newArrayList();
        for (int i = 0; i < 15; ++i) {
            writer.write(UUID.randomUUID().toString() + "\n");
        }
        writeUuids(writer, 5, expectedValues);
        writer.flush();

        _executor.execute(_tailer);

        _readTrigger.waitForWait();

        writeUuids(writer, 5, expectedValues);

        writer.close();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_positionStore, Mockito.atLeastOnce()).setPosition(Mockito.anyString(), Mockito.anyLong());
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileOpened();
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
        Mockito.verifyNoMoreInteractions(_listener);
    }

    @Test
    public void testRotateCopyTruncateLessData() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using copy-truncate (truncate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.copy(_file, oldFile.toPath());

        // Delay truncating the new file
        // NOTE: This ensures the reader does not read duplicate data either from the old or new file

        // Write _less_ data to the new file
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
            writeUuids(writer, 4, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // One additional wait because of rotation
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateCopyTruncateEqualLengthData() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }
        final BasicFileAttributes attributes = Files.readAttributes(_file, BasicFileAttributes.class);

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using copy-truncate (truncate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.copy(_file, oldFile.toPath());

        // Delay truncating the new file
        // NOTE: This ensures the reader does not read duplicate data either from the old or new file
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write _same length_ of data to the new file
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
            writeUuids(writer, 5, expectedValues);
        }

        // Ensure modified time stamps match
        // NOTE: This simulates a fast copy truncate within the precision of the file system time
        Files.setLastModifiedTime(_file, attributes.lastModifiedTime());

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateCopyTruncateMoreData() throws IOException, InterruptedException {
        // ** IMPORTANT **
        // It is not possible to determine that a file rotated if the new file
        // has more data than the old file within a single read interval. In
        // such a case the tailer will drop the data from the beginning of the
        // new file to the length of the old file. It is strongly recommended
        // the writers not use copy-truncate but instead use rename-recreate.
        // This filter demonstrates the broken functionality.
        // ** IMPORTANT **

        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using copy-truncate (truncate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.copy(_file, oldFile.toPath());

        // Delay truncating the new file
        // NOTE: This ensures the reader does not read duplicate data either from the old or new file
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write _same_ data to the new file
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int i = 0; i < 5; ++i) {
                writer.write(UUID.randomUUID().toString() + "\n");
            }
            writeUuids(writer, 5, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener, Mockito.never()).fileRotated();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateLessData() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using rename-recreate (recreate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Delay truncating the new file, causing a fileNotFound call
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Delay for fileNotFound
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write _less_ data to the new file
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 4, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Delay for file rotate
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.atLeastOnce()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateLessDataWriteToOldAfterRotate() throws IOException, InterruptedException {
        final BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW);
        final List<String> expectedValues = Lists.newArrayList();
        writeUuids(writerOld, 5, expectedValues);

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using rename-recreate (recreate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Delay creating the new file
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write some _more_ data to the old file
        writeUuids(writerOld, 5, expectedValues);
        writerOld.close();

        // Write _less_ data to the new file
        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerNew, 4, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Delay for the file rotate
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateLessDataWriteToOldAfterRotateNoDelay() throws IOException, InterruptedException {
        final BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW);
        final List<String> expectedValues = Lists.newArrayList();
        writeUuids(writerOld, 5, expectedValues);

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using rename-recreate
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Write some _more_ data to the old file
        writeUuids(writerOld, 5, expectedValues);
        writerOld.close();

        // Write _less_ data to the new file
        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerNew, 4, expectedValues);
        }

        // There is one interval for reading from the old file, another
        // interval for reading from the new file and then up to half an
        // interval left over from the original wait.
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateEqualData() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }
        final BasicFileAttributes attributes = Files.readAttributes(_file, BasicFileAttributes.class);

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using rename-recreate (recreate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Delay creating the new file, causing a fileNotFound call
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Additional delay for fileNotFound
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write _less_ data to the new file
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }

        // Ensure modified time stamps match
        // NOTE: This simulates a fast rename-recreate within the precision of the file system time
        Files.setLastModifiedTime(_file, attributes.lastModifiedTime());

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Additional delay for file rotate
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.atLeastOnce()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateEqualDataWriteToOldAfterRotate() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerOld, 5, expectedValues);

            _executor.execute(_tailer);
            _readTrigger.waitForWait();

            // Rotate files using rename-recreate (recreate is delayed; see below)
            final File oldFile = Files.createTempFile(_directory, "", "").toFile();
            Files.deleteIfExists(oldFile.toPath());
            Files.move(_file, oldFile.toPath());

            // Delay creating the new file, causing a fileNotFound call
            _readTrigger.releaseTrigger();
            _readTrigger.waitForWait();

            // Write some _more_ data to the old file
            writeUuids(writerOld, 5, expectedValues);
        }

        // Write _equal_ data to the new file
        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerNew, 10, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Rotate causes additional delay
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateEqualDataWriteToOldAfterRotateNoDelay() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerOld, 5, expectedValues);

            _executor.execute(_tailer);
            _readTrigger.waitForWait();

            // Rotate files using rename-recreate
            final File oldFile = Files.createTempFile(_directory, "", "").toFile();
            Files.deleteIfExists(oldFile.toPath());
            Files.move(_file, oldFile.toPath());

            // Write some _more_ data to the old file
            writeUuids(writerOld, 5, expectedValues);
        }

        // Write _equal_ data to the new file
        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerNew, 10, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Rotate causes additional delay
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateMoreData() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }

        _executor.execute(_tailer);

        _readTrigger.waitForWait();

        // Rotate files using rename-recreate (recreate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Delay creating the new file, causing a fileNotFound call
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // fileNotFound causes another wait
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write _more_ data to the new file
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 10, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Rotate causes a delay
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.atLeastOnce()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateMoreDataWithCheckpointing() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 5, expectedValues);
        }

        _executor.execute(_tailer);
        _readTrigger.waitForWait();

        // Rotate files using rename-recreate (recreate is delayed; see below)
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Delay creating the new file, causing a fileNotFound call
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // fileNotFound causes another wait
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        // Write data to the new file sufficient to checkpoint
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writer, 15, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Rotate causes a delay
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.atLeastOnce()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.atLeastOnce()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateMoreDataWriteToOldAfterRotate() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerOld, 5, expectedValues);

            _executor.execute(_tailer);

            _readTrigger.waitForWait();

            // Rotate files using copy rename-recreate (recreate is delayed; see below)
            final File oldFile = Files.createTempFile(_directory, "", "").toFile();
            Files.deleteIfExists(oldFile.toPath());
            Files.move(_file, oldFile.toPath());

            // Write some _more_ data to the old file
            writeUuids(writerOld, 5, expectedValues);
        }

        // Write data to the new file
        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerNew, 11, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testRotateRenameRecreateMoreDataWriteToOldAfterRotateNoDelay() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerOld, 5, expectedValues);

            _executor.execute(_tailer);

            _readTrigger.waitForWait();

            // Rotate files using copy rename-recreate
            final File oldFile = Files.createTempFile(_directory, "", "").toFile();
            Files.deleteIfExists(oldFile.toPath());
            Files.move(_file, oldFile.toPath());

            // Write some _more_ data to the old file
            writeUuids(writerOld, 5, expectedValues);
        }

        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            // Write _more_ data to the new file
            writeUuids(writerNew, 11, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileRotated();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
    }

    @Test
    public void testFailureToRotate() throws IOException, InterruptedException {
        // ** IMPORTANT **
        // This filter case demonstrates how a rotated file with the same content
        // as the original file will fail to rotate. This is not considered a
        // major issue because as soon as the new file diverges from the old
        // file then all the content in the new file will be read. However, it
        // does present a possible (and potentially indefinite) delay in the
        // retrieval of the old data. In the absolute worst case if the new
        // file is rotated again without ever being read that data is lost.
        // ** IMPORTANT **

        final List<String> expectedValues = Lists.newArrayList();
        try (BufferedWriter writerOld = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(writerOld, 5, expectedValues);
        }
        final BasicFileAttributes attributes = Files.readAttributes(_file, BasicFileAttributes.class);

        _executor.execute(_tailer);

        _readTrigger.waitForWait();

        // Rotate files using rename-recreate
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Write _exact same_ data to the new file
        try (BufferedWriter writerNew = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            for (int i = 0; i < 5; ++i) {
                writerNew.write(expectedValues.get(i) + "\n");
            }
        }

        // Ensure modified time stamps match
        // NOTE: This simulates a fast rename/recreate + write within the precision of the file system time
        Files.setLastModifiedTime(_file, attributes.lastModifiedTime());

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener).fileOpened();
        Mockito.verify(_positionStore, Mockito.never()).getPosition(Mockito.anyString());
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }

        // BUG 1: File rotation is not detected
        Mockito.verify(_listener, Mockito.never()).fileRotated();
        // Should be:
        //Mockito.verify(_listener).fileRotated();

        // BUG 2: Data from new file is never read
        Mockito.verifyNoMoreInteractions(_listener);
        // Should be:
        //for (final String expectedValue : expectedValues) {
        //    Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        //}
    }

    @Test
    public void testTailFromEnd() throws IOException, InterruptedException {
        // Ignore the first initialize invocation
        Mockito.verify(_listener).initialize(_tailer);
        final StatefulTailer.Builder builder = new StatefulTailer.Builder()
                .setListener(_listener)
                .setFile(_file)
                .setPositionStore(_positionStore)
                .setInitialPosition(InitialPosition.END)
                .setReadInterval(READ_INTERVAL);

        final List<String> expectedValues = Lists.newArrayList();
        _tailer = new StatefulTailer(builder, _readTrigger);
        Mockito.doNothing().when(_positionStore).setPosition(Mockito.anyString(), Mockito.anyLong());
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            for (int i = 0; i < 15; ++i) {
                writer.write(UUID.randomUUID().toString() + "\n");
            }
            writer.flush();

            _executor.execute(_tailer);
            _readTrigger.waitForWait();

            writeUuids(writer, 5, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).fileOpened();
        Mockito.verify(_listener).initialize(_tailer);
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
        Mockito.verifyNoMoreInteractions(_listener);
    }

    @Test
    public void testTailFromEndFirstFileOnly() throws IOException, InterruptedException {
        final List<String> expectedValues = Lists.newArrayList();

        // Ignore the first initialize invocation
        Mockito.verify(_listener).initialize(_tailer);
        final StatefulTailer.Builder builder = new StatefulTailer.Builder()
                .setListener(_listener)
                .setFile(_file)
                .setPositionStore(_positionStore)
                .setInitialPosition(InitialPosition.END)
                .setReadInterval(READ_INTERVAL);

        _tailer = new StatefulTailer(builder, _readTrigger);
        Mockito.doNothing().when(_positionStore).setPosition(Mockito.anyString(), Mockito.anyLong());
        try (BufferedWriter writer = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {

            for (int i = 0; i < 15; ++i) {
                writer.write(UUID.randomUUID().toString() + "\n");
            }
            writer.flush();

            _executor.execute(_tailer);
            _readTrigger.waitForWait();

            writeUuids(writer, 5, expectedValues);
        }

        // Rotate files using rename-recreate
        final File oldFile = Files.createTempFile(_directory, "", "").toFile();
        Files.deleteIfExists(oldFile.toPath());
        Files.move(_file, oldFile.toPath());

        // Write _more_ data to the new file
        try (BufferedWriter newWriter = Files.newBufferedWriter(_file, Charsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            writeUuids(newWriter, 10, expectedValues);
        }

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();
        // Rotate causes additional delay
        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        _readTrigger.releaseTrigger();
        _readTrigger.waitForWait();

        _readTrigger.disable();

        _tailer.stop();
        _executor.shutdown();
        _executor.awaitTermination(EXECUTOR_TERMINATE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Mockito.verify(_listener, Mockito.never()).handle(Mockito.any(Throwable.class));
        Mockito.verify(_listener, Mockito.never()).fileNotFound();
        Mockito.verify(_listener).initialize(_tailer);
        Mockito.verify(_listener, Mockito.times(2)).fileOpened();
        Mockito.verify(_listener).fileRotated();
        for (final String expectedValue : expectedValues) {
            Mockito.verify(_listener).handle(expectedValue.getBytes(Charsets.UTF_8));
        }
        Mockito.verifyNoMoreInteractions(_listener);
    }

    private static void writeUuids(final BufferedWriter writer, final int count, final List<String> values)
            throws IOException {
        for (int i = 0; i < count; ++i) {
            final String value = UUID.randomUUID().toString();
            values.add(value);
            LOGGER.debug(String.format("Writing; line=%s", value));
            writer.write(value + "\n");
        }
    }

    private Path _directory;
    private Path _file;
    private StatefulTailer _tailer;
    private ManualSingleThreadedTrigger _readTrigger;

    private final PositionStore _positionStore = Mockito.mock(PositionStore.class);
    private final TailerListener _listener = Mockito.mock(TailerListener.class);
    private final ExecutorService _executor = Executors.newSingleThreadExecutor();

    private static final Duration READ_INTERVAL = Duration.ofMillis(250);
    private static final Duration EXECUTOR_TERMINATE_TIMEOUT = Duration.ofMinutes(10);
    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulTailerTest.class);
}
