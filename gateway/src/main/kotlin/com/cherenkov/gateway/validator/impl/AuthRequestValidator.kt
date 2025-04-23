package com.cherenkov.gateway.validator.impl

import com.cherenkov.gateway.exception.ValidationException
import com.cherenkov.gateway.validator.RequestValidator
import org.slf4j.LoggerFactory
import org.springframework.http.HttpMethod
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

@Component
class AuthRequestValidator : RequestValidator {
    
    private val logger = LoggerFactory.getLogger(AuthRequestValidator::class.java)
    
    override fun supports(path: String, method: HttpMethod): Boolean {
        return (path.endsWith("/auth/login") || path.endsWith("/auth/register")) && 
                method == HttpMethod.POST
    }
    
    override fun validate(request: ServerHttpRequest): Mono<Void> {
        val contentType = request.headers.contentType
        
        if (contentType == null || !contentType.toString().contains("application/json")) {
            logger.warn("Validation failed: Invalid content type $contentType")
            return Mono.error(ValidationException("Content-Type must be application/json"))
        }
        logger.debug("Проверка базовых заголовков запроса прошла успешно: ${request.path}")
        return Mono.empty()
    }
} 