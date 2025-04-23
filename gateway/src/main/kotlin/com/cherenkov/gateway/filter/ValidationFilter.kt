package com.cherenkov.gateway.filter

import com.cherenkov.gateway.exception.ValidationException
import com.cherenkov.gateway.validator.RequestValidator
import org.slf4j.LoggerFactory
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.core.Ordered
import org.springframework.http.HttpMethod
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

@Component
class ValidationFilter(
    private val validators: List<RequestValidator>
) : GlobalFilter, Ordered {

    private val logger = LoggerFactory.getLogger(ValidationFilter::class.java)

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> {
        val request = exchange.request
        val path = request.path.toString()
        val method = request.method

        // Для GET и OPTIONS запросов обычно валидация не требуется
        if (method == HttpMethod.GET || method == HttpMethod.OPTIONS) {
            return chain.filter(exchange)
        }
        
        // Находим подходящие валидаторы для текущего пути и метода
        val matchingValidators = validators.filter { it.supports(path, method) }
        
        if (matchingValidators.isEmpty()) {
            // Если нет подходящих валидаторов, просто пропускаем запрос
            return chain.filter(exchange)
        }
        
        // Запускаем валидацию
        return validateRequest(request, matchingValidators)
            .then(chain.filter(exchange))
            .onErrorResume { error ->
                logger.error("Validation error: ${error.message}")
                if (error is ValidationException) {
                    Mono.error(error)
                } else {
                    Mono.error(ValidationException("Request validation failed: ${error.message}"))
                }
            }
    }
    
    override fun getOrder(): Int = Ordered.HIGHEST_PRECEDENCE + 10

    private fun validateRequest(
        request: ServerHttpRequest,
        validators: List<RequestValidator>
    ): Mono<Void> {
        // Последовательно применяем все валидаторы
        return validators.fold(Mono.empty()) { mono, validator ->
            mono.then(validator.validate(request))
        }
    }
} 