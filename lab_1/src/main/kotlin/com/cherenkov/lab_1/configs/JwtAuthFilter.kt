package com.cherenkov.lab_1.configs

import com.cherenkov.lab_1.service.JwtService
import com.cherenkov.lab_1.service.UserService
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.servlet.HandlerExceptionResolver

@Component
class JwtAuthFilter(
    private val jwtService: JwtService,
    private val userService: UserService
) : OncePerRequestFilter() {

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        // Не применяем фильтр для путей аутентификации
        val requestPath = request.requestURI
        if (requestPath.startsWith("/api/auth")) {
            filterChain.doFilter(request, response)
            return
        }
        
        val authHeader = request.getHeader("Authorization")
        
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            // Если нет токена и путь требует аутентификации, продолжаем цепочку
            // Дальнейшую проверку выполнит Spring Security
            filterChain.doFilter(request, response)
            return
        }
        
        try {
            val jwt = authHeader.substring(7)
            val username = jwtService.extractUsername(jwt)
            
            if (username != null && SecurityContextHolder.getContext().authentication == null) {
                // Проверяем, существует ли пользователь
                if (userService.userExists(username)) {
                    val userDetails = userService.loadUserByUsername(username)
                    
                    if (jwtService.isTokenValid(jwt, userDetails)) {
                        val authToken = UsernamePasswordAuthenticationToken(
                            userDetails,
                            null,
                            userDetails.authorities
                        )
                        authToken.details = WebAuthenticationDetailsSource().buildDetails(request)
                        SecurityContextHolder.getContext().authentication = authToken
                    }
                }
            }
            
            filterChain.doFilter(request, response)
        } catch (e: Exception) {
            // Ошибка валидации токена - пользователь не аутентифицирован
            // Spring Security обработает это и вернет 401/403
            filterChain.doFilter(request, response)
        }
    }
} 