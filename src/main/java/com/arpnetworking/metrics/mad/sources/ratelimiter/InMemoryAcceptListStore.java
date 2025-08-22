package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.google.common.collect.ImmutableSet;
import net.sf.oval.constraint.NotNull;

import java.util.HashSet;

public class InMemoryAcceptListStore implements AcceptListStore {
    private final HashSet<String> _cache = new HashSet<>();

    private InMemoryAcceptListStore(final Builder builder) {
        _cache.addAll(builder._initialCache);

    }

    @Override
    public ImmutableSet<String> getInitialList() {
        return ImmutableSet.copyOf(_cache);
    }

    @Override
    public ImmutableSet<String> updateAndReturnCurrent(final ImmutableSet<String> updated) {
        _cache.addAll(updated);
        return ImmutableSet.copyOf(_cache);
    }

    public static class Builder extends OvalBuilder<InMemoryAcceptListStore> {

        @NotNull
        private ImmutableSet<String> _initialCache = ImmutableSet.of();

        public Builder() {
            super(InMemoryAcceptListStore::new);
        }

        public Builder setInitialCache(final ImmutableSet<String> value) {
            _initialCache = value;
            return this;
        }
    }
}
