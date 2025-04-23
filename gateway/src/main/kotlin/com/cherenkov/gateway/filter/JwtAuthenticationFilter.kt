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

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request = exchange.request
        val path = request.path.toString()

        // Проверяем, нужна ли аутентификация для этого пути
        if (isPathExcluded(path)) {
            logger.debug("Path excluded from JWT validation: $path")
            return chain.filter(exchange)
        }

        val authHeader = request.headers.getFirst(HttpHeaders.AUTHORIZATION)
        
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            logger.debug("No valid Authorization header found for path: $path")
            exchange.response.statusCode = HttpStatus.UNAUTHORIZED
            return exchange.response.setComplete()
        }

        val jwt = authHeader.substring(7)
        
        return validateToken(jwt)
            .flatMap { username ->
                logger.debug("JWT validated for user: $username on path: $path")
                val authorities = listOf(SimpleGrantedAuthority("ROLE_USER"))
                val authentication = UsernamePasswordAuthenticationToken(username, null, authorities)
                
                // Добавляем информацию о пользователе в заголовки запроса
                val modifiedRequest = addUserInfoToRequest(request, username)
                val modifiedExchange = exchange.mutate().request(modifiedRequest).build()

                // Устанавливаем аутентификацию в контекст безопасности
                chain.filter(modifiedExchange)
                    .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authentication))
            }
            .onErrorResume { e ->
                logger.error("JWT validation failed: ${e.message}")
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
                Mono.error(RuntimeException("Invalid JWT token"))
            }
        } catch (e: Exception) {
            logger.error("Error validating JWT: ${e.message}")
            Mono.error(e)
        }
    }

    private fun isPathExcluded(path: String): Boolean {
        return excludePaths.any { path.startsWith(it) }
    }

    private fun addUserInfoToRequest(request: ServerHttpRequest, username: String): ServerHttpRequest {
        return request.mutate()
            .header("X-Auth-User", username)
            .build()
    }
} 