package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableSet;
import com.mercateo.test.clock.TestClock;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RedisAcceptListStoreTest extends TestCase {

    @Mock
    private Jedis _mockJedis;

    private TestClock _clock;
    private Instant _start;
    private RedisAcceptListStore _store;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        _start = Clock.systemUTC().instant();
        _clock = TestClock.fixed(_start, ZoneOffset.UTC);

        _store = new RedisAcceptListStore.Builder()
                .setClock(_clock)
                .setJedis(_mockJedis)
                .build();
    }

    @Test
    public void testInitializeReadsCurrentAndPrevious() {
        final String expectedKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).toString();
        final String expectedPreviousKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS).toString();
        when(_mockJedis.smembers(eq(expectedKey))).thenReturn(ImmutableSet.of("test1"));
        when(_mockJedis.smembers(eq(expectedPreviousKey))).thenReturn(ImmutableSet.of("test2"));
        final ImmutableSet<String> result = _store.getInitialList();
        verify(_mockJedis).smembers(eq(expectedKey));
        verify(_mockJedis).smembers(eq(expectedPreviousKey));
        assertEquals(2, result.size());
        assertTrue(result.contains("test1"));
        assertTrue(result.contains("test2"));
    }

    @Test
    public void testUsesInitialPreviousSet() {
        final String expectedKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).toString();
        final String expectedPreviousKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS).toString();
        when(_mockJedis.smembers(eq(expectedKey))).thenReturn(ImmutableSet.of("test1"));
        when(_mockJedis.smembers(eq(expectedPreviousKey))).thenReturn(ImmutableSet.of("test2"));
        final ImmutableSet<String> result = _store.getInitialList();
        assertEquals(2, result.size());
        assertTrue(result.contains("test1"));
        assertTrue(result.contains("test2"));

        reset(_mockJedis);
        when(_mockJedis.smembers(eq(expectedKey))).thenReturn(ImmutableSet.of("test3"));
        when(_mockJedis.sadd(eq(expectedKey), any())).thenReturn(1L);
        final ImmutableSet<String> updateResult = _store.updateAndReturnCurrent(ImmutableSet.of("test3"));
        verify(_mockJedis).smembers(eq(expectedKey));
        verify(_mockJedis).sadd(eq(expectedKey), eq("test3"));
        verifyNoMoreInteractions(_mockJedis);
        assertEquals(2, updateResult.size());
        assertTrue(updateResult.contains("test3"));
        assertTrue(updateResult.contains("test2"));
    }

    @Test
    public void testUpdatesPreviousSetOnKeyChange() {
        String expectedKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).toString();
        String expectedPreviousKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS).toString();
        when(_mockJedis.smembers(eq(expectedKey))).thenReturn(ImmutableSet.of("test1"));
        when(_mockJedis.smembers(eq(expectedPreviousKey))).thenReturn(ImmutableSet.of("test2"));
        final ImmutableSet<String> result = _store.getInitialList();
        assertEquals(2, result.size());
        assertTrue(result.contains("test1"));
        assertTrue(result.contains("test2"));

        _clock.fastForward(Duration.ofDays(1));

        reset(_mockJedis);
        expectedKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).toString();
        expectedPreviousKey = _clock.instant().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS).toString();
        when(_mockJedis.smembers(eq(expectedKey))).thenReturn(ImmutableSet.of("test3"));
        when(_mockJedis.smembers(eq(expectedPreviousKey))).thenReturn(ImmutableSet.of("test1"));
        when(_mockJedis.sadd(eq(expectedKey), any())).thenReturn(1L);
        final ImmutableSet<String> updateResult = _store.updateAndReturnCurrent(ImmutableSet.of("test3"));
        verify(_mockJedis).smembers(eq(expectedKey));
        verify(_mockJedis).expireAt(eq(expectedKey), eq(_clock.instant().truncatedTo(ChronoUnit.DAYS).plus(2, ChronoUnit.DAYS).getEpochSecond()));
        verify(_mockJedis).smembers(eq(expectedPreviousKey));
        verify(_mockJedis).sadd(eq(expectedKey), eq("test3"));
        verifyNoMoreInteractions(_mockJedis);
        assertEquals(2, updateResult.size());
        assertTrue(updateResult.contains("test3"));
        assertTrue(updateResult.contains("test1"));
    }
}