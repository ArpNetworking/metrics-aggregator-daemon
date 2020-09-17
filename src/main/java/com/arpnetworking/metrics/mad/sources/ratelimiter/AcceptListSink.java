package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableSet;

public interface AcceptListSink {
    void updateAcceptList(ImmutableSet<String> acceptList);
}
