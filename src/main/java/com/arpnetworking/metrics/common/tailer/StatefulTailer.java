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
package com.arpnetworking.metrics.common.tailer;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.utility.TimerTrigger;
import com.arpnetworking.utility.Trigger;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import net.sf.oval.constraint.NotNull;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.joda.time.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * A reimplementation of the Apache Commons IO tailer based on the 2.5 snapshot
 * version. This version attempts to address several shortcomings of the Apache
 * Commons implementation. In particular, more robust support for rename-
 * recreate file rotations and some progress for copy-truncate cases. The major
 * new feature is the <code>PositionStore</code> which is used to checkpoint
 * the offset in the tailed file as identified by a hash of the file prefix.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class StatefulTailer implements Tailer {

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        _isRunning = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(
                (thread, throwable) -> LOGGER.error()
                        .setMessage("Unhandled exception")
                        .setThrowable(throwable)
                        .log());

        try {
            fileLoop();
        } finally {
            IOUtils.closeQuietly(_positionStore);
            IOUtils.closeQuietly(_lineBuffer);
        }
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.<String, Object>builder()
                .put("file", _file)
                .put("positionStore", _positionStore)
                .put("listener", _listener)
                .put("isRunning", _isRunning)
                .put("trigger", _trigger)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /**
     * Determine if the <code>Tailer</code> is running.
     *
     * @return <code>True</code> if and only if the <code>Tailer</code> is running.
     */
    protected boolean isRunning() {
        return _isRunning;
    }

    private void fileLoop() {
        SeekableByteChannel reader = null;
        InitialPosition nextInitialPosition = _initialPosition;
        try {
            while (isRunning()) {
                // Attempt to open the file
                try {
                    reader = Files.newByteChannel(_file, StandardOpenOption.READ);
                    LOGGER.trace()
                            .setMessage("Opened file")
                            .addData("file", _file)
                            .log();
                } catch (final NoSuchFileException e) {
                    _listener.fileNotFound();
                    _trigger.waitOnTrigger();
                }

                if (reader != null) {
                    // Position the reader
                    resume(reader, nextInitialPosition);
                    _listener.fileOpened();

                    // Any subsequent file opens we should start at the beginning
                    nextInitialPosition = InitialPosition.START;

                    // Read the file
                    readLoop(reader);

                    // Reset per file state
                    IOUtils.closeQuietly(reader);
                    reader = null;
                    _hash = Optional.absent();
                }
            }
        // Clients may elect to kill the stateful tailer on an exception by calling stop, or they
        // may log the exception and continue. In the latter case it is strongly recommended that
        // clients pause before continuing; otherwise, if the error persists the stateful tailer
        // may create non-trivial load on the io subsystem.
        // NOTE: Any non-exception throwable will kill the stateful tailer.
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            handleThrowable(e);
            // CHECKSTYLE.OFF: IllegalCatch - Allow clients to decide how to handle exceptions
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            handleThrowable(e);
        } finally {
            IOUtils.closeQuietly(reader);
            reader = null;
            _hash = Optional.absent();
        }
    }

    private void resume(final SeekableByteChannel reader, final InitialPosition initialPosition) throws IOException {
        // Attempt to resume from checkpoint
        long position = initialPosition.get(reader);

        // Override position with last known position from store
        _hash = computeHash(reader, REQUIRED_BYTES_FOR_HASH);
        if (_hash.isPresent()) {
            final Optional<Long> storedPosition = _positionStore.getPosition(_hash.get());
            if (storedPosition.isPresent()) {
                // Optionally limit the size of the backlog to process
                final long fileSize = reader.size();
                if (_maximumOffsetOnResume.isPresent() && fileSize - storedPosition.get() > _maximumOffsetOnResume.get()) {
                    position = fileSize - _maximumOffsetOnResume.get();
                    // TODO(vkoskela): Discard the current potentially partial line [AINT-584]
                } else {
                    position = storedPosition.get();
                }
            }
        }

        LOGGER.info()
                .setMessage("Starting tailer")
                .addData("file", _file)
                .addData("position", position)
                .log();

        reader.position(position);
    }

    // CHECKSTYLE.OFF: MethodLength - Nothing to refactor here.
    private void readLoop(final SeekableByteChannel reader) throws IOException, InterruptedException {
        Optional<Long> lastChecked = Optional.absent();
        Optional<String> currentReaderPrefixHash = Optional.absent();
        int currentReaderPrefixHashLength = 0;
        while (isRunning()) {
            // Obtain properties of file we expect we are reading
            final Attributes attributes;
            try {
                attributes = getAttributes(_file, lastChecked);
            } catch (final NoSuchFileException t) {
                rotate(
                        Optional.of(reader),
                        String.format(
                                "File rotation detected based attributes access failure; file=%s",
                                _file));

                // Return to the file loop
                return;
            }

            if (attributes.getLength() < reader.position()) {
                // File was rotated; either:
                // 1) Position is past the length of the file
                // 2) The expected file is smaller than the current file
                rotate(
                        Optional.of(reader),
                        String.format(
                                "File rotation detected based on length, position and size; file=%s, length=%d, position=%d, size=%d",
                                _file,
                                attributes.getLength(),
                                reader.position(),
                                reader.size()));

                // Return to the file loop
                return;

            } else {
                // File was _likely_ not rotated
                if (reader.size() > reader.position()) {
                    // There is more data in the file
                    if (!readLines(reader)) {
                        // There actually isn't any more data in the file; this
                        // means the file was rotated and the new file has more
                        // data than the old file (e.g. rotation from empty).

                        // TODO(vkoskela): Account for missing final newline. [MAI-322]
                        // There is a degenerate case where the last line in a
                        // file does not have a newline. Then readLines will
                        // always find new data, but the file has been rotated
                        // away. We should buffer the contents of partial lines
                        // thereby detecting when the length grows whether we
                        // actually got more data in the current file.

                        rotate(
                                Optional.<SeekableByteChannel>absent(),
                                String.format(
                                        "File rotation detected based on length and no new data; file=%s, length=%d, position=%d",
                                        _file,
                                        attributes.getLength(),
                                        reader.position()));

                        // Return to the file loop
                        return;
                    }
                    try {
                        lastChecked = Optional.of(Files.getLastModifiedTime(_file).toMillis());
                    } catch (final NoSuchFileException t) {
                        rotate(
                                Optional.of(reader),
                                String.format(
                                        "File rotation detected based last modified time access failure; file=%s",
                                        _file));

                        // Return to the file loop
                        return;
                    }

                    // This control path, specifically, successfully reading
                    // data from the file does not trigger a wait. This permits
                    // continuous reading without pausing.

                } else if (attributes.isNewer()) {
                    // The file does not contain any additional data, but its
                    // last modified date is after the last read date. The file
                    // must have rotated and contains the same length of
                    // content. This can happen on periodic systems which log
                    // the same data at the beginning of each period.

                    rotate(
                            Optional.<SeekableByteChannel>absent(),
                            String.format(
                                    "File rotation detected based equal length and position but newer"
                                            + "; file=%s, length=%d, position=%d, lastChecked=%s, attributes=%s",
                                    _file,
                                    attributes.getLength(),
                                    reader.position(),
                                    lastChecked.get(),
                                    attributes));

                    // Return to the file loop
                    return;

                } else {
                    // The files are the same size and the timestamps are the
                    // same. This is more common than it sounds since file
                    // modification timestamps are not very precise on many
                    // file systems.
                    //
                    // Since we're not doing anything at this point let's hash
                    // the first N bytes of the current file and the expected
                    // file to see if we're still working on the same file.

                    final Optional<Boolean> hashesSame = compareByHash(currentReaderPrefixHash, currentReaderPrefixHashLength);
                    if (hashesSame.isPresent() && !hashesSame.get()) {
                        // The file rotated with the same length!
                        rotate(
                                Optional.<SeekableByteChannel>absent(),
                                String.format(
                                        "File rotation detected based on hash; file=%s",
                                        _file));

                        // Return to the file loop
                        return;
                    }
                    // else: the files are empty or the hashes are the same. In
                    // either case we don't have enough data to determine if
                    // the files are different; we'll need to wait and see when
                    // more data is written if the size and age diverge.

                    // TODO(vkoskela): Configurable maximum rotation hash size. [MAI-323]
                    // TODO(vkoskela): Configurable minimum rotation hash size. [MAI-324]
                    // TODO(vkoskela): Configurable identity hash size. [MAI-325]
                    // TODO(vkoskela): We should add a rehash interval. [MAI-326]
                    // This interval would be separate from the read interval,
                    // and generally longer, preventing us from rehashing the
                    // file every interval; but short enough that we don't wait
                    // too long before realizing a slowly growing file was
                    // rotated.

                    // Read interval
                    _trigger.waitOnTrigger();
                }
            }

            // Compute the prefix hash unless we have an identity
            final int newPrefixHashLength = (int) Math.min(reader.size(), REQUIRED_BYTES_FOR_HASH);
            if (!_hash.isPresent() && (currentReaderPrefixHashLength != newPrefixHashLength || !currentReaderPrefixHash.isPresent())) {
                currentReaderPrefixHashLength = newPrefixHashLength;
                currentReaderPrefixHash = computeHash(reader, currentReaderPrefixHashLength);
            }

            // Update the reader position
            updateCheckpoint(reader.position());
        }
    }
    // CHECKSTYLE.ON: MethodLength

    private Attributes getAttributes(final Path file, final Optional<Long> lastChecked) throws IOException {
        final BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
        LOGGER.trace()
                .setMessage("File attributes")
                .addData("file", file)
                .addData("lastModifiedTime", attributes.lastModifiedTime().toMillis())
                .addData("size", attributes.size())
                .log();

        return new Attributes(
                attributes.size(),
                attributes.lastModifiedTime().toMillis(),
                lastChecked.isPresent() && attributes.lastModifiedTime().toMillis() > lastChecked.get());
    }

    private void rotate(final Optional<SeekableByteChannel> reader, final String reason) throws InterruptedException, IOException {
        // Allow a full read interval before calling it quits on the old file
        if (reader.isPresent()) {
            _trigger.waitOnTrigger();
            readLines(reader.get());
        }

        // Inform the listener
        _listener.fileRotated();

        LOGGER.info(reason);
    }

    private boolean readLines(final SeekableByteChannel reader) throws IOException {
        // Compute the hash if not already set
        if (!_hash.isPresent() && reader.size() >= REQUIRED_BYTES_FOR_HASH) {
            _hash = computeHash(reader, REQUIRED_BYTES_FOR_HASH);
        }

        // Track current position in file and next read position
        // NOTE: The next read position is always the beginning of a line
        long position = reader.position();
        long nextReadPosition = position;

        // Reset buffers
        _buffer.clear();
        _lineBuffer.reset();

        // Process available data
        int bufferSize = reader.read(_buffer);
        boolean hasData = false;
        boolean hasCR = false;
        while (isRunning() && bufferSize != -1) {
            hasData = true;
            for (int i = 0; i < bufferSize; i++) {
                final byte ch = _buffer.get(i);
                switch (ch) {
                    case '\n':
                        hasCR = false;
                        handleLine();
                        nextReadPosition = position + i + 1;
                        updateCheckpoint(nextReadPosition);
                        break;
                    case '\r':
                        if (hasCR) {
                            _lineBuffer.write('\r');
                        }
                        hasCR = true;
                        break;
                    default:
                        if (hasCR) {
                            hasCR = false;
                            handleLine();
                            nextReadPosition = position + i + 1;
                            updateCheckpoint(nextReadPosition);
                        }
                        _lineBuffer.write(ch);
                }
            }
            position = reader.position();
            _buffer.clear();
            bufferSize = reader.read(_buffer);
        }

        reader.position(nextReadPosition);
        return hasData;
    }

    private Optional<Boolean> compareByHash(final Optional<String> prefixHash, final int prefixLength) {
        final int appliedLength;
        if (_hash.isPresent()) {
            appliedLength = REQUIRED_BYTES_FOR_HASH;
        } else {
            appliedLength = prefixLength;
        }
        try (final SeekableByteChannel reader = Files.newByteChannel(_file, StandardOpenOption.READ)) {
            final Optional<String> filePrefixHash = computeHash(
                    reader,
                    appliedLength);

            LOGGER.trace()
                    .setMessage("Comparing hashes")
                    .addData("hash1", prefixHash)
                    .addData("filePrefixHash", filePrefixHash)
                    .addData("size", appliedLength)
                    .log();

            return Optional.of(Objects.equals(_hash.or(prefixHash).orNull(), filePrefixHash.orNull()));
        } catch (final IOException e) {
            return Optional.absent();
        }
    }

    private Optional<String> computeHash(final SeekableByteChannel reader, final int hashSize) throws IOException {
        // Don't hash empty data sets
        if (hashSize <= 0) {
            return Optional.absent();
        }

        // Validate sufficient data to compute the hash
        final long oldPosition = reader.position();
        reader.position(0);
        if (reader.size() < hashSize) {
            reader.position(oldPosition);
            LOGGER.trace()
                    .setMessage("Reader size insufficient to compute hash")
                    .addData("hashSize", hashSize)
                    .addData("readerSize", reader.size())
                    .log();
            return Optional.absent();
        }

        // Read the data to hash
        final ByteBuffer buffer = ByteBuffer.allocate(hashSize);
        int totalBytesRead = 0;
        while (totalBytesRead < hashSize) {
            final int bytesRead = reader.read(buffer);
            if (bytesRead < 0) {
                LOGGER.warn()
                        .setMessage("Unexpected end of file reached")
                        .addData("totalBytesRead", totalBytesRead)
                        .log();
                return Optional.absent();
            }
            totalBytesRead += bytesRead;
        }

        // Compute the hash
        _md5.reset();
        final byte[] digest = _md5.digest(buffer.array());
        final String hash = Hex.encodeHexString(digest);
        LOGGER.trace()
                .setMessage("Computed hash")
                .addData("hash", hash)
                .log();

        // Return the reader to its original state
        reader.position(oldPosition);
        return Optional.of(hash);
    }

    private void updateCheckpoint(final long position) {
        if (_hash.isPresent()) {
            _positionStore.setPosition(_hash.get(), position);
        }
    }

    private void handleLine() {
        _listener.handle(_lineBuffer.toByteArray());
        _lineBuffer.reset();
    }

    private void handleThrowable(final Throwable t) {
        _listener.handle(t);
    }

    // NOTE: Package private for testing

    /* package private */ StatefulTailer(final Builder builder, final Trigger trigger) {
        _file = builder._file;
        _positionStore = builder._positionStore;
        _listener = builder._listener;
        _trigger = trigger;

        _buffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        _lineBuffer = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        try {
            _md5 = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }

        _initialPosition = builder._initialPosition;
        _maximumOffsetOnResume = Optional.fromNullable(builder._maximumOffsetOnResume);
        _listener.initialize(this);
    }

    private StatefulTailer(final Builder builder) {
        // TODO(vkoskela): Configurable grace period separate from interval. [MAI-327]
        this(builder, new TimerTrigger(builder._readInterval));
    }

    private final Path _file;
    private final PositionStore _positionStore;
    private final TailerListener _listener;
    private final ByteBuffer _buffer;
    private final ByteArrayOutputStream _lineBuffer;
    private final MessageDigest _md5;
    private final InitialPosition _initialPosition;
    private final Optional<Long> _maximumOffsetOnResume;
    private final Trigger _trigger;

    private volatile boolean _isRunning = true;
    private Optional<String> _hash = Optional.absent();

    private static final int REQUIRED_BYTES_FOR_HASH = 512;
    private static final int INITIAL_BUFFER_SIZE = 65536;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulTailer.class);

    private static final class Attributes {

        private Attributes(
                final long length,
                final long lastModifiedTime,
                final boolean newer) {
            _length = length;
            _lastModifiedTime = lastModifiedTime;
            _newer = newer;
        }

        public long getLength() {
            return _length;
        }

        public long getLastModifiedTime() {
            return _lastModifiedTime;
        }

        public boolean isNewer() {
            return _newer;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", Integer.toHexString(System.identityHashCode(this)))
                    .add("Length", _length)
                    .add("LastModifiedTime", _lastModifiedTime)
                    .add("Newer", _newer)
                    .toString();
        }

        private final long _length;
        private final long _lastModifiedTime;
        private final boolean _newer;
    }

    /**
     * Implementation of builder pattern for <code>StatefulTailer</code>.
     *
     * @author Brandon Arp (brandonarp at gmail dot com)
     */
    public static class Builder extends OvalBuilder<StatefulTailer> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder, StatefulTailer>) StatefulTailer::new);
        }

        /**
         * Sets the file to read. Cannot be null or empty.
         *
         * @param value The file to read.
         * @return This instance of {@link Builder}
         */
        public Builder setFile(final Path value) {
            _file = value;
            return this;
        }

        /**
         * Sets the <code>PositionStore</code> to be used to checkpoint the
         * file read position. Cannot be null.
         *
         * @param value The <code>PositionStore</code> instance.
         * @return This instance of {@link Builder}
         */
        public Builder setPositionStore(final PositionStore value) {
            _positionStore = value;
            return this;
        }

        /**
         * Sets the <code>TailerListener</code> instance. Cannot be null.
         *
         * @param value The <code>TailerListener</code> instance.
         * @return This instance of {@link Builder}
         */
        public Builder setListener(final TailerListener value) {
            _listener = value;
            return this;
        }

        /**
         * Sets the interval between file reads. Optional. Default is 500
         * milliseconds.
         *
         * @param value The file read interval.
         * @return This instance of {@link Builder}
         */
        public Builder setReadInterval(final Duration value) {
            _readInterval = value;
            return this;
        }

        /**
         * Sets the tailer to start at the current end of the file.
         *
         * @param initialPosition The initial position of the tailer
         * @return This instance of {@link Builder}
         */
        public Builder setInitialPosition(final InitialPosition initialPosition) {
            _initialPosition = initialPosition;
            return this;
        }

        /**
         * Sets the maximum offset on resume. Optional. Default is no maximum.
         *
         * @param maximumOffsetOnResume The maximum offset on resume.
         * @return This instance of {@link Builder}
         */
        public Builder setMaximumOffsetOnResume(final Long maximumOffsetOnResume) {
            _maximumOffsetOnResume = maximumOffsetOnResume;
            return this;
        }

        @NotNull
        private Path _file;
        @NotNull
        private PositionStore _positionStore;
        @NotNull
        private TailerListener _listener;
        @NotNull
        private Duration _readInterval = Duration.millis(250);
        @NotNull
        private InitialPosition _initialPosition = InitialPosition.START;
        private Long _maximumOffsetOnResume = null;
    }
}
