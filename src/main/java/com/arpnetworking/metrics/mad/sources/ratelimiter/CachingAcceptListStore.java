package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import net.sf.oval.constraint.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class CachingAcceptListStore implements AcceptListStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(CachingAcceptListStore.class);
    private final AcceptListStore _store;
    private HashSet<String> _cache;
    private final ObjectMapper _objectMapper = ObjectMapperFactory.getInstance();
    private final File _cacheFile;
    private final File _backupFile;
    private boolean _cacheGood = false;

    private CachingAcceptListStore(Builder builder) {
        _store = builder._store;

        _cacheFile = new File(builder._filePath);
        _backupFile = new File(builder._filePath + builder._backupExtension);
        // Try to load the cache file first
        TypeReference<HashSet<String>> _typeRef = new TypeReference<HashSet<String>>() {
        };
        if (_cacheFile.exists() && _cacheFile.isFile()) {
            try {
                _cache = _objectMapper.readValue(_cacheFile, _typeRef);
                _cacheGood = true;
            } catch (IOException e) {
                LOGGER.warn()
                        .setMessage("Failed to load cache file.")
                        .addData("file", _cacheFile)
                        .log();
            }
        }

        // Failed to load the cache file, try the backup
        if (_cache == null && _backupFile.exists() && _backupFile.isFile()) {
            try {
                _cache = _objectMapper.readValue(_backupFile, _typeRef);
            } catch (IOException e) {
                LOGGER.warn()
                        .setMessage("Failed to load backup cache file.")
                        .addData("file", _backupFile)
                        .log();
            }
        }

        // Unable to load any file, start with an empty set
        if (_cache == null) {
            LOGGER.warn()
                    .setMessage("Unable to load cache file.")
                    .log();
            _cache = new HashSet<>();
        }
    }

    @Override
    public ImmutableSet<String> getInitialList() {
       return ImmutableSet.copyOf(_cache);
    }

    @Override
    public ImmutableSet<String> updateAndReturnCurrent(final ImmutableSet<String> updated) {
        // We update our local cache with whatever was provided and persist it, if we get a response
        // from the underlying store we replace what we just persisted with the store's contents.
        // In the case that the store throws an exception, we simply return the cached response.
        // This doesn't handle a true cold start case where the underlying store is empty.


        ImmutableSet<String> result = _store.updateAndReturnCurrent(updated);

        if( result != null) {
            _cache = new HashSet<>(result);
        } else {
            _cache.addAll(updated);
        }

        File tmpFile;
        try {
            tmpFile = File.createTempFile("mad_origin_cache", ".tmp");
            tmpFile.deleteOnExit();
            _objectMapper.writeValue(tmpFile, _cache);
        } catch (IOException e) {
            tmpFile = null;
            LOGGER.warn()
                    .setMessage("Failed to create tmp cache file.")
                    .setThrowable(e)
                    .log();
        }

        if (tmpFile != null) {
            // The primary cache file was 'good' so move it to the backup, otherwise we ignore it
            if (_cacheGood) {
                // Move the current file to become the backup, persist to the current file
                if (!_cacheFile.renameTo(_backupFile)) {
                    LOGGER.warn()
                            .setMessage("Failed to move cache file to backup.")
                            .addData("file", _cacheFile)
                            .addData("backup", _backupFile)
                            .log();
                }
            }

            // Try and move the tmp file to the main cache file
            if (!tmpFile.renameTo(_cacheFile)) {
                LOGGER.warn()
                        .setMessage("Failed to move temp cache file.")
                        .addData("file", _cacheFile)
                        .addData("tmpFile", tmpFile)
                        .log();
            } else {
                _cacheGood = true;
            }

            if (!tmpFile.delete()) {
                LOGGER.warn()
                        .setMessage("Failed to delete temp cache file.")
                        .addData("tmpFile", tmpFile)
                        .log();
            }
        }

        return result != null ? result: ImmutableSet.copyOf(_cache);
    }


    /**
     * Implementation of builder pattern for <code>ReverseRateLimitingSource</code>.
     *
     * @author Gil Markham (gmarkham at dropbox dot com)
     */
    public static class Builder extends OvalBuilder<CachingAcceptListStore> {
        public Builder() {
            super(CachingAcceptListStore::new);
        }

        /**
         * Sets wrappped store. Cannot be null.
         *
         * @param value The distinct origin store.
         * @return This instance of <code>Builder</code>.
         */
        public final Builder setStore(final AcceptListStore value) {
            _store = value;
            return this;
        }

        /**
         * Sets the filepath to the local filesystem cache.
         *
         * @param value filepath of cache file
         * @return This instance of <code>Builder</code>.
         */
        public final Builder setFilePath(final String value) {
            _filePath = value;
            return this;
        }

        /**
         * Sets the backup file extension for the local filesystem cache, defaults to '.bak'
         *
         * @param value backup file extension
         * @return This instance of <code>Builder</code>.
         */
        public final Builder setBackupExtension(final String value) {
            _backupExtension = value;
            return this;
        }

        @NotNull
        private AcceptListStore _store;
        @NotNull
        private String _filePath;
        @NotNull
        private String _backupExtension = ".bak";
    }

}
