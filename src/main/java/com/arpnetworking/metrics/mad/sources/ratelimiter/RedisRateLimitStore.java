package com.arpnetworking.metrics.mad.sources.ratelimiter;

import com.google.common.collect.ImmutableMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisRateLimitStore implements RateLimitStore {
    private final Jedis _jedis = new Jedis();
    private final Map<String, Long> _cache = new HashMap<>();

    @Override
    public ImmutableMap<String, Long> updateAndReturnCurrent(Map<String, Long> recent) {
        final Long nextExpiry = 0L;
        final Map<String, Response<Long>> responseMap = recent.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            final Transaction transaction = _jedis.multi();
                            final String key = entry.getKey();
                            final Response<Long> response = transaction.incrBy(key, entry.getValue());
                            transaction.expireAt(key, nextExpiry);
                            transaction.exec();
                            return response;
                        }
                ));


        responseMap.forEach((key, value) -> _cache.compute(key, (k, v) -> v == null ? value.get() : v + value.get()));

        return ImmutableMap.copyOf(_cache);
    }
}
