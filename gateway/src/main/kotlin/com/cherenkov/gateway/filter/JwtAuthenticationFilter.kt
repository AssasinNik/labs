package com.cherenkov.gateway.filter

import com.cherenkov.gateway.service.JwtService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class JwtAuthenticationFilter(
    private val jwtService: JwtService
) : WebFilter {

    private val logger = LoggerFactory.getLogger(JwtAuthenticationFilter::class.java)
    
    @Value("\${gateway.auth.exclude-paths}")
    private lateinit var excludePaths: List<String>
    
    @Value("\${gateway.auth.header-name:X-Gateway-Auth}")
    private lateinit var gatewayHeaderName: String
    
    @Value("\${gateway.auth.header-value:true}")
    private lateinit var gatewayHeaderValue: String

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request = exchange.request
        val path = request.path.toString()

        // Логирование пути
        logger.debug("Processing request with path: $path")
        
        // Проверяем, является ли путь исключением
        if (isPathExcluded(path)) {
            logger.debug("Path excluded from JWT validation: $path")
            // Добавляем заголовок Gateway для путей исключений
            val modifiedRequest = addGatewayAuthHeader(request)
            val modifiedExchange = exchange.mutate().request(modifiedRequest).build()
            return chain.filter(modifiedExchange)
        }

        val authHeader = request.headers.getFirst(HttpHeaders.AUTHORIZATION)
        
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            logger.warn("No valid Authorization header found for path: $path")
            exchange.response.statusCode = HttpStatus.UNAUTHORIZED
            return exchange.response.setComplete()
        }

        val jwt = authHeader.substring(7)
        
        return validateToken(jwt)
            .flatMap { username ->
                logger.info("JWT validated for user: $username on path: $path")
                val authorities = listOf(SimpleGrantedAuthority("ROLE_USER"))
                val authentication = UsernamePasswordAuthenticationToken(username, null, authorities)
                
                // Добавляем информацию о пользователе и заголовок Gateway в запрос
                val modifiedRequest = addUserInfoAndGatewayHeader(request, username)
                val modifiedExchange = exchange.mutate().request(modifiedRequest).build()

                // Устанавливаем аутентификацию в контекст безопасности
                chain.filter(modifiedExchange)
                    .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authentication))
            }
            .onErrorResume { e ->
                logger.error("JWT validation failed: ${e.message}", e)
                exchange.response.statusCode = HttpStatus.UNAUTHORIZED
                exchange.response.setComplete()
            }
    }

    private fun validateToken(token: String): Mono<String> {
        return try {
            val username = jwtService.extractUsername(token)
            if (jwtService.isTokenValid(token)) {
                Mono.just(username)
            } else {
                logger.warn("JWT token is invalid")
                Mono.error(RuntimeException("Invalid JWT token"))
            }
        } catch (e: Exception) {
            logger.error("Error validating JWT: ${e.message}", e)
            Mono.error(e)
        }
    }
    
    private fun isPathExcluded(path: String): Boolean {
        return excludePaths.any { excludePath ->
            // Проверяем точное соответствие или шаблоны с /**
            if (excludePath.endsWith("/**")) {
                val basePattern = excludePath.dropLast(3)
                path == basePattern || path.startsWith("$basePattern/")
            } else {
                path == excludePath || path.startsWith("$excludePath/") || path.startsWith("$excludePath?")
            }
        }
    }

    private fun addUserInfoAndGatewayHeader(request: ServerHttpRequest, username: String): ServerHttpRequest {
        return request.mutate()
            .header("X-Auth-User", username)
            .header(gatewayHeaderName, gatewayHeaderValue)
            .build()
    }
    
    private fun addGatewayAuthHeader(request: ServerHttpRequest): ServerHttpRequest {
        return request.mutate()
            .header(gatewayHeaderName, gatewayHeaderValue)
            .build()
    }
} 