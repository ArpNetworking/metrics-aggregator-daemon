package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableSet;
import net.sf.oval.constraint.NotNull;
import redis.clients.jedis.Jedis;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Set;

public class RedisAcceptListStore implements AcceptListStore {
    private final static Logger LOGGER = LoggerFactory.getLogger(RedisAcceptListStore.class);
    private final Jedis _jedis;
    private final Clock _clock;
    private String lastKey;
    private Set<String> previousSet;

    private RedisAcceptListStore(final Builder builder) {
        _jedis = builder._jedis;
        _clock = builder._clock;
    }

    @Override
    public ImmutableSet<String> getInitialList() {
        String key = getCurrentKey();
        final Set<String> currentSet = _jedis.smembers(key);
        previousSet = _jedis.smembers(getPreviousKey());
        lastKey = key;

        // Blend the previous set with the current set so that a recently created current
        // set doesn't artificially reject metrics
        return ImmutableSet.<String>builder()
                .addAll(currentSet)
                .addAll(previousSet)
                .build();
    }

    @Override
    public ImmutableSet<String> updateAndReturnCurrent(ImmutableSet<String> seen) {
        final String key = getCurrentKey();
        boolean updateExpiry = false;

        // If the 'current' key was updated or we weren't initialized (lastKey == null) then fetch the
        // previous accept list and cache it until the key is updated again.  The previous set shouldn't
        // be being written to at this point so it's okay to read it once.
        if (!key.equals(lastKey)) {
            previousSet = _jedis.smembers(getPreviousKey());
            lastKey = key;
            updateExpiry = true;
        }

        final long count = _jedis.sadd(key, seen.asList().toArray(new String[seen.size()]));
        LOGGER.info().setMessage("Added metrics to accept list").addData("count", count);
        if (updateExpiry) {
            _jedis.expireAt(key, currentExpiry());
        }

        final Set<String> currentSet = _jedis.smembers(key);

        // Blend the previous set with the current set so that a recently created current
        // set doesn't artificially reject metrics
        return ImmutableSet.<String>builder()
                .addAll(currentSet)
                .addAll(previousSet)
                .build();
    }

    private int currentExpiry() {
        return (int) _clock.instant()
                .truncatedTo(ChronoUnit.DAYS)
                .plus(2, ChronoUnit.DAYS)
                .getEpochSecond();
    }

    private String getCurrentKey() {
        return _clock.instant().truncatedTo(ChronoUnit.DAYS).toString();
    }

    private String getPreviousKey() {
        return _clock.instant().truncatedTo(ChronoUnit.DAYS).minus(1, ChronoUnit.DAYS).toString();
    }

    public static class Builder extends OvalBuilder<RedisAcceptListStore> {
        @NotNull
        private Jedis _jedis;
        @NotNull
        private Clock _clock = Clock.systemUTC();

        public Builder() {
            super(RedisAcceptListStore::new);
        }

        public Builder setJedis(final Jedis value) {
            _jedis = value;
            return this;
        }

        public Builder setClock(final Clock value) {
            _clock = value;
            return this;
        }
    }
}
