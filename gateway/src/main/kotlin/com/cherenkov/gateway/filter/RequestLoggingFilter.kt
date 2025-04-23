package com.cherenkov.gateway.filter

import org.slf4j.LoggerFactory
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.core.Ordered
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.util.*

@Component
class RequestLoggingFilter : GlobalFilter, Ordered {

    private val logger = LoggerFactory.getLogger(RequestLoggingFilter::class.java)

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> {
        val request = exchange.request
        val requestId = UUID.randomUUID().toString()
        
        // Логируем входящий запрос
        logRequest(exchange, requestId)
        
        // Добавляем requestId для отслеживания запроса
        val mutatedRequest = exchange.request.mutate()
            .header("X-Request-ID", requestId)
            .build()

        val mutatedExchange = exchange.mutate().request(mutatedRequest).build()
        
        // Логируем ответ после обработки запроса
        return chain.filter(mutatedExchange)
            .doOnSuccess { logResponse(exchange, requestId, null) }
            .doOnError { error -> logResponse(exchange, requestId, error) }
    }
    
    override fun getOrder(): Int = Ordered.HIGHEST_PRECEDENCE

    private fun logRequest(exchange: ServerWebExchange, requestId: String) {
        val request = exchange.request
        val method = request.method
        val uri = request.uri
        val headers = formatHeaders(request)
        
        logger.info(
            "REQUEST [{}]: {} {} {}",
            requestId, method, uri, headers
        )
    }
    
    private fun logResponse(exchange: ServerWebExchange, requestId: String, error: Throwable?) {
        val response = exchange.response
        val statusCode = response.statusCode
        
        if (error != null) {
            logger.error(
                "RESPONSE [{}]: Status={}, Error={}",
                requestId, statusCode, error.message, error
            )
        } else {
            logger.info(
                "RESPONSE [{}]: Status={}",
                requestId, statusCode
            )
        }
    }
    
    private fun formatHeaders(request: ServerHttpRequest): Map<String, String> {
        val result = mutableMapOf<String, String>()
        
        request.headers.forEach { (name, values) ->
            // Маскировка чувствительных заголовков
            val value = when {
                name.equals("Authorization", ignoreCase = true) -> "Bearer [MASKED]"
                name.equals("Cookie", ignoreCase = true) -> "[MASKED]"
                else -> values.joinToString(", ")
            }
            result[name] = value
        }
        
        return result
    }
} 