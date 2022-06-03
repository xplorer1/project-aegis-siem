package com.aegis.enrichment;

import com.aegis.domain.ThreatInfo;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;

/**
 * REST implementation of TipClient for calling external threat intelligence platform APIs.
 * 
 * Features:
 * - Circuit breaker pattern for fault tolerance
 * - Rate limiting to respect TIP API limits
 * - Automatic retry with exponential backoff
 * - Timeout handling (500ms default)
 * - Graceful degradation (returns SAFE on errors)
 */
@Component
public class TipClientRestImpl implements TipClient {
    
    private static final Logger log = LoggerFactory.getLogger(TipClientRestImpl.class);
    
    private final WebClient webClient;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiter rateLimiter;
    private final EnrichmentMetrics metrics;
    
    @Value("${aegis.tip.api.url:https://api.threatintel.example.com}")
    private String tipApiUrl;
    
    @Value("${aegis.tip.api.key:}")
    private String tipApiKey;
    
    @Value("${aegis.tip.timeout.ms:500}")
    private int timeoutMs;
    
    /**
     * Constructor initializes WebClient and resilience components
     */
    public TipClientRestImpl(EnrichmentMetrics metrics) {
        this.metrics = metrics;
        // Initialize WebClient with timeout
        this.webClient = WebClient.builder()
            .baseUrl(tipApiUrl)
            .build();
        
        // Configure circuit breaker
        // Opens after 5 failures in 10 seconds, half-open after 30 seconds
        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)  // Open if 50% of calls fail
            .waitDurationInOpenState(Duration.ofSeconds(30))  // Wait 30s before half-open
            .slidingWindowSize(10)  // Track last 10 calls
            .minimumNumberOfCalls(5)  // Need at least 5 calls to calculate failure rate
            .build();
        
        this.circuitBreaker = CircuitBreaker.of("tipClient", cbConfig);
        
        // Configure rate limiter
        // Limit to 100 requests per second to respect TIP API limits
        RateLimiterConfig rlConfig = RateLimiterConfig.custom()
            .limitForPeriod(100)  // 100 requests
            .limitRefreshPeriod(Duration.ofSeconds(1))  // per second
            .timeoutDuration(Duration.ofMillis(100))  // Wait max 100ms for permit
            .build();
        
        this.rateLimiter = RateLimiter.of("tipClient", rlConfig);
    }
    
    @Override
    public Mono<ThreatInfo> lookup(String ip) {
        metrics.recordTipApiCall();
        
        return webClient.get()
            .uri("/v1/ip/{ip}", ip)
            .header("X-API-Key", tipApiKey)
            .retrieve()
            .bodyToMono(ThreatInfo.class)
            .timeout(Duration.ofMillis(timeoutMs))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(RateLimiterOperator.of(rateLimiter))
            .retryWhen(Retry.backoff(2, Duration.ofMillis(100))
                .filter(this::isRetryableError))
            .onErrorResume(e -> {
                metrics.recordTipApiError();
                return handleError(e);
            })
            .doOnSuccess(info -> log.debug("TIP lookup for IP {} returned: {}", ip, info.getThreatCategory()))
            .doOnError(e -> log.warn("TIP lookup failed for IP {}: {}", ip, e.getMessage()));
    }
    
    @Override
    public Mono<ThreatInfo> lookupDomain(String domain) {
        metrics.recordTipApiCall();
        
        return webClient.get()
            .uri("/v1/domain/{domain}", domain)
            .header("X-API-Key", tipApiKey)
            .retrieve()
            .bodyToMono(ThreatInfo.class)
            .timeout(Duration.ofMillis(timeoutMs))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(RateLimiterOperator.of(rateLimiter))
            .retryWhen(Retry.backoff(2, Duration.ofMillis(100))
                .filter(this::isRetryableError))
            .onErrorResume(e -> {
                metrics.recordTipApiError();
                return handleError(e);
            })
            .doOnSuccess(info -> log.debug("TIP lookup for domain {} returned: {}", domain, info.getThreatCategory()))
            .doOnError(e -> log.warn("TIP lookup failed for domain {}: {}", domain, e.getMessage()));
    }
    
    @Override
    public Mono<ThreatInfo> lookupHash(String hash) {
        metrics.recordTipApiCall();
        
        return webClient.get()
            .uri("/v1/hash/{hash}", hash)
            .header("X-API-Key", tipApiKey)
            .retrieve()
            .bodyToMono(ThreatInfo.class)
            .timeout(Duration.ofMillis(timeoutMs))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(RateLimiterOperator.of(rateLimiter))
            .retryWhen(Retry.backoff(2, Duration.ofMillis(100))
                .filter(this::isRetryableError))
            .onErrorResume(e -> {
                metrics.recordTipApiError();
                return handleError(e);
            })
            .doOnSuccess(info -> log.debug("TIP lookup for hash {} returned: {}", hash, info.getThreatCategory()))
            .doOnError(e -> log.warn("TIP lookup failed for hash {}: {}", hash, e.getMessage()));
    }
    
    /**
     * Determine if an error is retryable.
     * Retry on network errors and 5xx server errors, but not on 4xx client errors.
     */
    private boolean isRetryableError(Throwable throwable) {
        if (throwable instanceof WebClientResponseException) {
            WebClientResponseException ex = (WebClientResponseException) throwable;
            int status = ex.getStatusCode().value();
            
            // Retry on 429 (rate limit) and 5xx (server errors)
            // Don't retry on 4xx (client errors like 404, 401)
            return status == 429 || status >= 500;
        }
        
        // Retry on network errors
        return true;
    }
    
    /**
     * Handle errors gracefully by returning SAFE threat info.
     * This ensures the enrichment pipeline doesn't fail on TIP errors.
     */
    private Mono<ThreatInfo> handleError(Throwable throwable) {
        if (throwable instanceof WebClientResponseException) {
            WebClientResponseException ex = (WebClientResponseException) throwable;
            
            // 404 means not found in TIP database - treat as safe
            if (ex.getStatusCode().value() == 404) {
                return Mono.just(ThreatInfo.SAFE);
            }
            
            // 429 means rate limited - log and return safe
            if (ex.getStatusCode().value() == 429) {
                log.warn("TIP API rate limit exceeded");
                return Mono.just(ThreatInfo.SAFE);
            }
        }
        
        // For all other errors, log and return safe to avoid blocking the pipeline
        log.error("TIP lookup error: {}", throwable.getMessage());
        return Mono.just(ThreatInfo.SAFE);
    }
}
