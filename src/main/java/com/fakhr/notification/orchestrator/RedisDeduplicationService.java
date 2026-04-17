package com.fakhr.notification.orchestrator;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RedisDeduplicationService {

    private static final String DEDUP_KEY_FORMAT = "dedup:%s:%s";
    private static final Duration DEDUP_TTL = Duration.ofHours(24);
    private static final String DEDUP_MARKER_VALUE = "1";

    private final StringRedisTemplate stringRedisTemplate;

    public RedisDeduplicationService(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public boolean isDuplicate(String idempotencyKey, String clientId) {
        String dedupKey = DEDUP_KEY_FORMAT.formatted(clientId, idempotencyKey);

        // setIfAbsent + TTL performs a single atomic dedup write and expiration in Redis.
        Boolean wasCreated = stringRedisTemplate.opsForValue()
                .setIfAbsent(dedupKey, DEDUP_MARKER_VALUE, DEDUP_TTL);

        return !Boolean.TRUE.equals(wasCreated);
    }
}
