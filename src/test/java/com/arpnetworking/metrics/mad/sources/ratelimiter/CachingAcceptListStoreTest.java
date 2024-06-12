package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachingAcceptListStoreTest extends TestCase {

    @Mock
    private AcceptListStore _mockStore;
    @Captor
    private ArgumentCaptor<ImmutableSet<String>> _listCaptor;

    private final ObjectMapper _objectMapper = ObjectMapperFactory.getInstance();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testLoadsCacheFromDisk() throws IOException {
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        primaryFile.deleteOnExit();
        _objectMapper.writeValue(primaryFile, ImmutableSet.of("foo"));


        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        final ImmutableSet<String> storeList = store.getInitialList();
        Assert.assertNotNull(storeList);
        Assert.assertEquals(1, storeList.size());
        Assert.assertTrue(storeList.contains("foo"));
    }

    @Test
    public void testLoadsBackupCacheFromDiskIfPrimaryIsMissing() throws IOException {
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        primaryFile.delete();
        final File backupFile = new File(primaryFile.getAbsolutePath() + ".bak");
        backupFile.deleteOnExit();

        _objectMapper.writeValue(backupFile, ImmutableSet.of("foo"));


        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        final ImmutableSet<String> storeList = store.getInitialList();
        Assert.assertNotNull(storeList);
        Assert.assertEquals(1, storeList.size());
        Assert.assertTrue(storeList.contains("foo"));
    }

    @Test
    public void testLoadsBackupCacheFromDiskIfPrimaryIsBroken() throws IOException {
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        primaryFile.deleteOnExit();
        final FileWriter primaryFileWriter = new FileWriter(primaryFile);
        primaryFileWriter.write("123123");
        primaryFileWriter.close();
        final File backupFile = new File(primaryFile.getAbsolutePath() + ".bak");
        backupFile.deleteOnExit();

        _objectMapper.writeValue(backupFile, ImmutableSet.of("foo"));


        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        final ImmutableSet<String> storeList = store.getInitialList();
        Assert.assertNotNull(storeList);
        Assert.assertEquals(1, storeList.size());
        Assert.assertTrue(storeList.contains("foo"));
    }

    @Test
    public void testFallsBackToEmptyIfCacheFilesAreMissing() throws IOException {
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        primaryFile.delete();

        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        final ImmutableSet<String> storeList = store.getInitialList();
        Assert.assertNotNull(storeList);
        Assert.assertEquals(0, storeList.size());
    }

    @Test
    public void testFallsBackToEmptyIfBothFilesAreBroken() throws IOException {
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        primaryFile.deleteOnExit();
        final FileWriter primaryFileWriter = new FileWriter(primaryFile);
        primaryFileWriter.write("123123");
        primaryFileWriter.close();

        final File backupFile = new File(primaryFile.getAbsolutePath() + ".bak");
        backupFile.deleteOnExit();
        final FileWriter backupFileWriter = new FileWriter(backupFile);
        backupFileWriter.write("123123");
        backupFileWriter.close();

        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        final ImmutableSet<String> storeList = store.getInitialList();
        Assert.assertNotNull(storeList);
        Assert.assertEquals(0, storeList.size());
    }

    @Test
    public void testUpdateCallsUnderlyingStoreAndUpdatesCacheFiles() throws IOException {
        when(_mockStore.updateAndReturnCurrent(any())).thenReturn(ImmutableSet.of("foo", "bar"));
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        final long createDate = primaryFile.lastModified();
        primaryFile.deleteOnExit();
        final File backupFile = new File(primaryFile.getAbsolutePath() + ".bak");
        backupFile.createNewFile();
        backupFile.deleteOnExit();
        //assertFalse(backupFile.exists());

        _objectMapper.writeValue(primaryFile, ImmutableSet.of("foo"));


        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        store.updateAndReturnCurrent(ImmutableSet.of("bar"));
        verify(_mockStore).updateAndReturnCurrent(_listCaptor.capture());

        final ImmutableSet<String> result = _listCaptor.getValue();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(createDate < primaryFile.lastModified());
        assertTrue(backupFile.exists());
    }

    @Test
    public void testUpdateCallsUnderlyingStoreAndUpdatesPrimaryOnlyIfWhenBad() throws IOException {
        when(_mockStore.updateAndReturnCurrent(any())).thenReturn(ImmutableSet.of("foo", "bar"));
        final File primaryFile = File.createTempFile("CachingAcceptListStoreTest", ".json");
        final FileWriter primaryFileWriter = new FileWriter(primaryFile);
        primaryFileWriter.write("123124312");
        primaryFileWriter.close();
        final long createDate = primaryFile.lastModified();
        primaryFile.deleteOnExit();

        final File backupFile = new File(primaryFile.getAbsolutePath() + ".bak");
        backupFile.deleteOnExit();

        _objectMapper.writeValue(backupFile, ImmutableSet.of("foo"));
        assertTrue(backupFile.exists());

        final long backupModifiedDate = backupFile.lastModified();


        final CachingAcceptListStore store = new CachingAcceptListStore.Builder()
                .setStore(_mockStore)
                .setFilePath(primaryFile.getAbsolutePath())
                .setBackupExtension(".bak")
                .build();

        store.updateAndReturnCurrent(ImmutableSet.of("bar"));
        verify(_mockStore).updateAndReturnCurrent(_listCaptor.capture());

        final ImmutableSet<String> result = _listCaptor.getValue();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(createDate < primaryFile.lastModified());
        assertEquals(backupModifiedDate, backupFile.lastModified());
    }
}