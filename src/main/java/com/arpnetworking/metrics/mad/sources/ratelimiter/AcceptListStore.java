package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableSet;

public interface AcceptListStore {
    ImmutableSet<String> getInitialList();
    ImmutableSet<String> updateAndReturnCurrent(final ImmutableSet<String> updated);
}
