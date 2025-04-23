package com.cherenkov.gateway.filter

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.core.Ordered
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Фильтр для сбора метрик о запросах, проходящих через API Gateway
 */
@Component
class MetricsFilter(private val meterRegistry: MeterRegistry) : GlobalFilter, Ordered {

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> {
        val startTime = System.nanoTime()
        val path = exchange.request.path.value()
        val method = exchange.request.method.name()
        
        // Увеличиваем счетчик общего количества запросов
        meterRegistry.counter("gateway.requests.total", "path", path, "method", method).increment()
        
        return chain.filter(exchange)
            .doOnSuccess {
                recordMetrics(exchange, startTime, true)
            }
            .doOnError { error ->
                recordMetrics(exchange, startTime, false)
                meterRegistry.counter(
                    "gateway.requests.errors", 
                    "path", path, 
                    "method", method, 
                    "exception", error.javaClass.simpleName
                ).increment()
            }
    }
    
    private fun recordMetrics(exchange: ServerWebExchange, startTime: Long, isSuccess: Boolean) {
        val path = exchange.request.path.value()
        val method = exchange.request.method.name()
        val status = exchange.response.statusCode?.value() ?: 0
        val endTime = System.nanoTime()
        val executionTime = endTime - startTime
        
        // Регистрируем время выполнения запроса
        Timer.builder("gateway.requests.duration")
            .description("Request processing time")
            .tags(
                "path", path,
                "method", method,
                "status", status.toString(),
                "success", isSuccess.toString()
            )
            .register(meterRegistry)
            .record(executionTime, TimeUnit.NANOSECONDS)
        
        // Регистрируем статусы ответов
        meterRegistry.counter(
            "gateway.requests.status", 
            "path", path, 
            "method", method, 
            "status", status.toString()
        ).increment()
    }
    
    override fun getOrder(): Int = Ordered.LOWEST_PRECEDENCE
} 