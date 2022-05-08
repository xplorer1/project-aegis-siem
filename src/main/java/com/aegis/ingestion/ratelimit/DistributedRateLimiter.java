package com.aegis.ingestion.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Collections;

/**
 * Distributed rate limiter using Redis and token bucket algorithm.
 * Provides per-tenant rate limiting with atomic operations via Lua scripts.
 */
@Component
public class DistributedRateLimiter {
    
    private final RedisTemplate<String, String> redis;
    
    @Value("${aegis.ratelimit.capacity:10000}")
    private long capacity;
    
    @Value("${aegis.ratelimit.refill-rate:1000}")
    private long refillRate;
    
    // Metrics
    private final Counter acceptedCounter;
    private final Counter rejectedCounter;
    
    // Lua script for atomic token bucket check-and-decrement
    private static final String LUA_SCRIPT = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local rate = tonumber(ARGV[2])
        local requested = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])
        
        local bucket = redis.call('HMGET', key, 'tokens', 'last_update')
        local tokens = tonumber(bucket[1]) or capacity
        local last_update = tonumber(bucket[2]) or now
        
        -- Refill tokens based on elapsed time
        local elapsed = now - last_update
        tokens = math.min(capacity, tokens + (elapsed * rate))
        
        if tokens >= requested then
            tokens = tokens - requested
            redis.call('HMSET', key, 'tokens', tokens, 'last_update', now)
            redis.call('EXPIRE', key, 3600)
            return 1
        else
            return 0
        end
    """;
    
    @Autowired
    public DistributedRateLimiter(RedisTemplate<String, String> redis, MeterRegistry meterRegistry) {
        this.redis = redis;
        
        // Initialize metrics
        this.acceptedCounter = Counter.builder("aegis.ratelimit.accepted")
                .description("Number of requests accepted by rate limiter")
                .register(meterRegistry);
        this.rejectedCounter = Counter.builder("aegis.ratelimit.rejected")
                .description("Number of requests rejected by rate limiter")
                .register(meterRegistry);
    }
    
    /**
     * Try to acquire a token for the given tenant
     * @param tenantId The tenant identifier
     * @return Mono<Boolean> true if token acquired, false if rate limited
     */
    public Mono<Boolean> tryAcquire(String tenantId) {
        String key = "ratelimit:" + tenantId;
        long requested = 1;
        long now = System.currentTimeMillis() / 1000;
        
        return Mono.fromCallable(() -> {
            Long result = redis.execute(
                RedisScript.of(LUA_SCRIPT, Long.class),
                Collections.singletonList(key),
                String.valueOf(capacity),
                String.valueOf(refillRate),
                String.valueOf(requested),
                String.valueOf(now)
            );
            boolean acquired = result != null && result == 1L;
            
            // Track metrics
            if (acquired) {
                acceptedCounter.increment();
            } else {
                rejectedCounter.increment();
            }
            
            return acquired;
        });
    }
}
