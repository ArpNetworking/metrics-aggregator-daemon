package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public interface RateLimitStore {
    ImmutableMap<String, Long> updateAndReturnCurrent(Map<String, Long> recent);
}
