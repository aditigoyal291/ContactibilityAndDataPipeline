package com.neo4j.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class RateLimiterService {

    private final RedisTemplate<String, Object> redisTemplate;

    public boolean tryAcquire(String key, int limit, int seconds) {
        Long currentCount = redisTemplate.opsForValue().increment(key, 1);

        if (currentCount != null && currentCount == 1) {
            redisTemplate.expire(key, Duration.ofSeconds(seconds));
        }

        return currentCount != null && currentCount <= limit;
    }
}
