package com.fakhr.notification.orchestrator;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RedisRateLimiter {

    private static final String RATE_KEY_FORMAT = "rate:%s";
    private static final int MAX_REQUESTS_PER_MINUTE = 100;
    private static final Duration RATE_WINDOW_TTL = Duration.ofSeconds(60);

    private final StringRedisTemplate stringRedisTemplate;

    public RedisRateLimiter(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public boolean isRateLimited(String clientId) {
        String rateKey = RATE_KEY_FORMAT.formatted(clientId);
        Long requestCount = stringRedisTemplate.opsForValue().increment(rateKey);

        if (requestCount == null) {
            return false;
        }

        if (requestCount == 1L) {
            stringRedisTemplate.expire(rateKey, RATE_WINDOW_TTL);
        }

        return requestCount > MAX_REQUESTS_PER_MINUTE;
    }
}
