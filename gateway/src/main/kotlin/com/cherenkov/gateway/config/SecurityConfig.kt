package com.cherenkov.gateway.config

import com.cherenkov.gateway.filter.JwtAuthenticationFilter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.SecurityWebFiltersOrder
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsConfigurationSource
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource

@Configuration
@EnableWebFluxSecurity
class SecurityConfig(
    private val jwtAuthenticationFilter: JwtAuthenticationFilter
) {

    private val logger = LoggerFactory.getLogger(SecurityConfig::class.java)

    @Value("\${gateway.auth.exclude-paths}")
    private lateinit var excludePaths: List<String>

    @Bean
    fun securityWebFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        logger.info("Configuring security with exclude paths: $excludePaths")
        
        return http
            .csrf { it.disable() }
            .cors { it.configurationSource(corsConfigurationSource()) }
            .authorizeExchange {
                // Явное разрешение для путей авторизации
                it.pathMatchers("/api/auth/login", "/api/auth/register", "/api/auth/refresh-token").permitAll()
                it.pathMatchers("/actuator/**").permitAll()
                it.pathMatchers("/fallback/**").permitAll()
                
                // Для всех остальных запросов требуется аутентификация
                it.anyExchange().authenticated()
            }
            .addFilterAt(jwtAuthenticationFilter, SecurityWebFiltersOrder.AUTHENTICATION)
            .httpBasic { it.disable() }
            .formLogin { it.disable() }
            .exceptionHandling {
                it.authenticationEntryPoint { exchange, ex ->
                    logger.warn("Authentication failure: {} for path: {}", ex.message, exchange.request.uri.path)
                    exchange.response.statusCode = org.springframework.http.HttpStatus.UNAUTHORIZED
                    exchange.response.setComplete()
                }
            }
            .build()
    }

    @Bean
    fun corsConfigurationSource(): CorsConfigurationSource {
        val configuration = CorsConfiguration()
        configuration.allowedOrigins = listOf("*")
        configuration.allowedMethods = listOf("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH")
        configuration.allowedHeaders = listOf("*")
        configuration.maxAge = 3600L

        val source = UrlBasedCorsConfigurationSource()
        source.registerCorsConfiguration("/**", configuration)
        logger.debug("CORS configuration applied")
        return source
    }
} 