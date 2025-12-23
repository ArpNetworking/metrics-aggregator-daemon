package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class InMemoryAcceptListStoreTest extends TestCase {

    @Test
    public void testUpdateAndReturnCurrent() {
        final InMemoryAcceptListStore store = new InMemoryAcceptListStore.Builder()
                .setInitialCache(ImmutableSet.of("foobar"))
                .build();
        ImmutableSet<String> returnVal = store.updateAndReturnCurrent(ImmutableSet.of("foo"));
        Assert.assertEquals(2, returnVal.size());
        Assert.assertTrue(returnVal.contains("foo"));
        Assert.assertTrue(returnVal.contains("foobar"));
        returnVal = store.updateAndReturnCurrent(ImmutableSet.of("foo", "bar"));
        Assert.assertEquals(3, returnVal.size());
        Assert.assertTrue(returnVal.contains("foo"));
        Assert.assertTrue(returnVal.contains("bar"));
        Assert.assertTrue(returnVal.contains("foobar"));
    }

    @Test
    public void testGetInitialList() {
        final InMemoryAcceptListStore store = new InMemoryAcceptListStore.Builder()
                .setInitialCache(ImmutableSet.of("foobar"))
                .build();
        final ImmutableSet<String> returnVal = store.getInitialList();
        Assert.assertEquals(1, returnVal.size());
        Assert.assertTrue(returnVal.contains("foobar"));
    }
}