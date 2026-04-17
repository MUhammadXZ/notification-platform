package com.fakhr.notification.consumer;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class Resilience4jConfig {

    private static final float FAILURE_RATE_THRESHOLD = 15.0f;
    private static final Duration WAIT_DURATION_IN_OPEN_STATE = Duration.ofSeconds(30);
    private static final int SLIDING_WINDOW_SIZE = 10;
    private static final int HALF_OPEN_PERMITTED_CALLS = 3;

    @Bean
    public CircuitBreakerConfig sharedCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(FAILURE_RATE_THRESHOLD)
                .waitDurationInOpenState(WAIT_DURATION_IN_OPEN_STATE)
                .slidingWindowSize(SLIDING_WINDOW_SIZE)
                .permittedNumberOfCallsInHalfOpenState(HALF_OPEN_PERMITTED_CALLS)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(CircuitBreakerConfig sharedCircuitBreakerConfig) {
        return CircuitBreakerRegistry.of(sharedCircuitBreakerConfig);
    }

    @Bean("emailCircuitBreaker")
    public CircuitBreaker emailCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        return circuitBreakerRegistry.circuitBreaker("emailCircuitBreaker");
    }

    @Bean("smsCircuitBreaker")
    public CircuitBreaker smsCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        return circuitBreakerRegistry.circuitBreaker("smsCircuitBreaker");
    }

    @Bean("pushCircuitBreaker")
    public CircuitBreaker pushCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        return circuitBreakerRegistry.circuitBreaker("pushCircuitBreaker");
    }
}
