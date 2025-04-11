package com.cherenkov.lab_1.security

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.web.util.matcher.AntPathRequestMatcher
import org.springframework.security.web.util.matcher.OrRequestMatcher
import org.springframework.security.web.util.matcher.RequestMatcher
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class JwtAuthFilter(
    private val jwtService: JwtService
) : OncePerRequestFilter() {
    
    private val logger = LoggerFactory.getLogger(JwtAuthFilter::class.java)
    
    private val publicPaths = OrRequestMatcher(
        AntPathRequestMatcher("/"),
        AntPathRequestMatcher("/auth/**"),
        AntPathRequestMatcher("/swagger-ui/**"),
        AntPathRequestMatcher("/v3/api-docs/**")
    )

    override fun shouldNotFilter(request: HttpServletRequest): Boolean {
        return publicPaths.matches(request)
    }

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        try {
            val authHeader = request.getHeader("Authorization")
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                logger.warn("Отсутствует или неверный формат токена в запросе: ${request.requestURI}")
                SecurityContextHolder.clearContext()
                filterChain.doFilter(request, response)
                return
            }
            
            val token = authHeader.substring(7)
            
            if (jwtService.validateAccessToken(token)) {
                val userId = jwtService.getUserIdFromToken(token)
                val auth = UsernamePasswordAuthenticationToken(userId, null, emptyList())
                SecurityContextHolder.getContext().authentication = auth
                logger.debug("Токен валидирован для пользователя $userId: ${request.requestURI}")
            } else {
                logger.warn("Невалидный JWT токен для запроса: ${request.requestURI}")
                SecurityContextHolder.clearContext()
            }
        } catch (e: Exception) {
            logger.error("Ошибка обработки JWT токена: ${e.message}")
            SecurityContextHolder.clearContext()
        }

        filterChain.doFilter(request, response)
    }
}