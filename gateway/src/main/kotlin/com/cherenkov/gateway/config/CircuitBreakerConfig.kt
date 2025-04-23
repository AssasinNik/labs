package com.cherenkov.gateway.config

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig as Resilience4jCircuitBreakerConfig
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder
import org.springframework.cloud.client.circuitbreaker.Customizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

/**
 * Конфигурация Circuit Breaker для API Gateway
 * Предотвращает каскадные отказы при недоступности сервисов
 */
@Configuration
class CircuitBreakerConfig {

    @Bean
    fun defaultCustomizer(): Customizer<ReactiveResilience4JCircuitBreakerFactory> {
        return Customizer { factory ->
            factory.configureDefault { id ->
                Resilience4JConfigBuilder(id)
                    .circuitBreakerConfig(
                        Resilience4jCircuitBreakerConfig.custom()
                            .slidingWindowSize(10)
                            .failureRateThreshold(50F)
                            .waitDurationInOpenState(Duration.ofSeconds(10))
                            .permittedNumberOfCallsInHalfOpenState(5)
                            .slowCallRateThreshold(50F)
                            .slowCallDurationThreshold(Duration.ofSeconds(2))
                            .build()
                    )
                    .timeLimiterConfig(
                        TimeLimiterConfig.custom()
                            .timeoutDuration(Duration.ofSeconds(3))
                            .build()
                    )
                    .build()
            }
            
            // Настройка Circuit Breaker для конкретных сервисов с разными параметрами
            factory.configure(
                { builder ->
                    builder
                        .circuitBreakerConfig(
                            Resilience4jCircuitBreakerConfig.custom()
                                .slidingWindowSize(5)
                                .failureRateThreshold(40F)
                                .waitDurationInOpenState(Duration.ofSeconds(30))
                                .build()
                        )
                        .timeLimiterConfig(
                            TimeLimiterConfig.custom()
                                .timeoutDuration(Duration.ofSeconds(5))
                                .build()
                        )
                },
                "lab-1-service"
            )
        }
    }
} 