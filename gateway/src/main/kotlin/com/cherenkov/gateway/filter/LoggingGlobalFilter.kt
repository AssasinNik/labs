package com.cherenkov.gateway.filter

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.core.Ordered
import org.springframework.http.HttpStatus
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.util.*

/**
 * Глобальный фильтр для расширенного логирования всех запросов
 * с поддержкой корреляционных ID и MDC для структурированного логирования
 */
@Component
class LoggingGlobalFilter : GlobalFilter, Ordered {

    private val logger = LoggerFactory.getLogger(LoggingGlobalFilter::class.java)

    companion object {
        const val CORRELATION_ID = "X-Correlation-ID"
        const val REQUEST_ID = "X-Request-ID"
    }

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> {
        val startTime = System.currentTimeMillis()
        val request = exchange.request
        
        // Получаем или генерируем корреляционный ID для отслеживания запроса
        val correlationId = getOrGenerateCorrelationId(request)
        
        // Устанавливаем корреляционный ID в MDC для логирования
        return Mono.fromCallable {
            MDC.put(CORRELATION_ID, correlationId)
            logRequest(request, correlationId)
            startTime
        }.then(Mono.defer {
            // Добавляем корреляционный ID в заголовки ответа
            exchange.response.headers.add(CORRELATION_ID, correlationId)
            
            val modifiedRequest = request.mutate()
                .header(CORRELATION_ID, correlationId)
                .build()
                
            val modifiedExchange = exchange.mutate().request(modifiedRequest).build()
            
            chain.filter(modifiedExchange)
                .doFinally { signalType ->
                    try {
                        val duration = System.currentTimeMillis() - startTime
                        logResponse(exchange, correlationId, duration)
                    } finally {
                        MDC.remove(CORRELATION_ID)
                    }
                }
        })
    }
    
    override fun getOrder(): Int = Ordered.HIGHEST_PRECEDENCE
    
    private fun getOrGenerateCorrelationId(request: ServerHttpRequest): String {
        // Используем существующий ID, если он есть в заголовках
        return request.headers.getFirst(CORRELATION_ID)
            ?: request.headers.getFirst(REQUEST_ID)
            ?: UUID.randomUUID().toString()
    }
    
    private fun logRequest(request: ServerHttpRequest, correlationId: String) {
        val method = request.method
        val uri = request.uri
        val path = request.path.toString()
        val queryParams = sanitizeQueryParams(request)
        val headers = sanitizeHeaders(request)
        
        logger.info(
            "REQUEST [{}]: {} {} from {}",
            correlationId, 
            method, 
            uri,
            request.remoteAddress?.address?.hostAddress ?: "unknown"
        )
        
        // Добавляем путь и параметры для лучшей отладки
        logger.debug("REQUEST PATH [{}]: {}", correlationId, path)
        if (queryParams.isNotEmpty()) {
            logger.debug("REQUEST PARAMS [{}]: {}", correlationId, queryParams)
        }
        
        if (logger.isDebugEnabled) {
            logger.debug("REQUEST HEADERS [{}]: {}", correlationId, headers)
        }
    }
    
    private fun logResponse(exchange: ServerWebExchange, correlationId: String, duration: Long) {
        val response = exchange.response
        val statusCode = response.statusCode ?: HttpStatus.INTERNAL_SERVER_ERROR
        
        val message = if (statusCode.is2xxSuccessful) {
            "RESPONSE [{}]: Status={}, Duration={}ms"
        } else {
            "ERROR RESPONSE [{}]: Status={}, Duration={}ms"
        }
        
        if (statusCode.is2xxSuccessful) {
            logger.info(message, correlationId, statusCode, duration)
        } else {
            // Для ошибок логируем на уровне WARN или ERROR
            if (statusCode.is4xxClientError) {
                logger.warn(message, correlationId, statusCode, duration)
                
                // Детальное логирование для ошибки 401 Unauthorized
                if (statusCode == HttpStatus.UNAUTHORIZED) {
                    val path = exchange.request.path.toString()
                    logger.warn(
                        "UNAUTHORIZED ACCESS [{}]: Path={}, Headers={}",
                        correlationId,
                        path,
                        sanitizeHeaders(exchange.request)
                    )
                }
            } else {
                logger.error(message, correlationId, statusCode, duration)
            }
        }
        
        // Логируем заголовки ответа при детальной отладке
        if (logger.isDebugEnabled) {
            logger.debug("RESPONSE HEADERS [{}]: {}", correlationId, response.headers)
        }
    }
    
    private fun sanitizeHeaders(request: ServerHttpRequest): Map<String, String> {
        val result = mutableMapOf<String, String>()
        
        request.headers.forEach { (name, values) ->
            // Маскируем конфиденциальные данные
            val value = when {
                name.equals("Authorization", ignoreCase = true) -> "Bearer [MASKED]"
                name.equals("Cookie", ignoreCase = true) -> "[MASKED]"
                name.equals("Set-Cookie", ignoreCase = true) -> "[MASKED]"
                else -> values.joinToString(", ")
            }
            result[name] = value
        }
        
        return result
    }
    
    private fun sanitizeQueryParams(request: ServerHttpRequest): Map<String, String> {
        val result = mutableMapOf<String, String>()
        
        request.queryParams.forEach { (name, values) ->
            // Маскируем конфиденциальные данные в параметрах запроса
            val value = when {
                name.equals("password", ignoreCase = true) -> "[MASKED]"
                name.equals("token", ignoreCase = true) -> "[MASKED]"
                name.equals("access_token", ignoreCase = true) -> "[MASKED]"
                name.equals("refresh_token", ignoreCase = true) -> "[MASKED]"
                else -> values.joinToString(", ")
            }
            result[name] = value
        }
        
        return result
    }
} 