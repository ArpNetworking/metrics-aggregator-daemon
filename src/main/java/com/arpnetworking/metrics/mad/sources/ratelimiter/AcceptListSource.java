package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableSet;

public interface AcceptListSource {
    ImmutableSet<String> getAcceptList();
}
