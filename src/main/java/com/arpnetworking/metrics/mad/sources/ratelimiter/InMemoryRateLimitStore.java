package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class InMemoryRateLimitStore implements RateLimitStore {
    private final HashMap<String, Long> _store = new HashMap<>();

    @Override
    public ImmutableMap<String, Long> updateAndReturnCurrent(Map<String, Long> recent) {
        recent.forEach((key,value) -> _store.compute(key, (k,v) -> v == null ? value : v + value));
        return ImmutableMap.copyOf(_store);
    }
}
