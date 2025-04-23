package com.cherenkov.gateway.validator

import org.springframework.http.HttpMethod
import org.springframework.http.server.reactive.ServerHttpRequest
import reactor.core.publisher.Mono

/**
 * Интерфейс для валидаторов запросов в API Gateway
 */
interface RequestValidator {
    
    /**
     * Проверяет, поддерживает ли валидатор указанный путь и метод
     *
     * @param path путь запроса
     * @param method HTTP метод запроса
     * @return true если валидатор поддерживает этот путь и метод
     */
    fun supports(path: String, method: HttpMethod): Boolean

    /**
     * Валидирует запрос
     *
     * @param request объект запроса
     * @return Mono<Void> в случае успешной валидации, или Mono.error если валидация не пройдена
     */
    fun validate(request: ServerHttpRequest): Mono<Void>
} 