package com.cherenkov.lab_2.filter

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.annotation.Order
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

/**
 * Фильтр для проверки заголовка, который устанавливается только API Gateway.
 * Этот фильтр гарантирует, что доступ к сервису возможен только через API Gateway,
 * а не напрямую к микросервису.
 */
@Component
@Order(0) // Высокий приоритет, чтобы выполнялся до других фильтров безопасности
class GatewayAuthFilter : OncePerRequestFilter() {

    private val logger = LoggerFactory.getLogger(GatewayAuthFilter::class.java)

    @Value("\${security.gateway.header.name:X-Gateway-Auth}")
    private lateinit var gatewayHeaderName: String
    
    @Value("\${security.gateway.header.value:true}")
    private lateinit var gatewayHeaderValue: String
    
    @Value("\${security.gateway.user.header:X-Auth-User}")
    private lateinit var userHeaderName: String

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val requestPath = request.requestURI

        // Получаем заголовок Gateway
        val gatewayAuthHeader = request.getHeader(gatewayHeaderName)
        val userHeader = request.getHeader(userHeaderName)

        if (gatewayAuthHeader == null || gatewayAuthHeader != gatewayHeaderValue) {
            logger.warn("Попытка прямого доступа к защищенному ресурсу без Gateway: $requestPath")
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Доступ к сервису разрешен только через API Gateway")
            return
        }

        // Если заголовок есть, значит запрос пришел через Gateway
        logger.debug("Запрос от Gateway на путь: $requestPath")
        
        // Если есть информация о пользователе, создаем аутентификацию
        if (userHeader != null) {
            val authorities = listOf(SimpleGrantedAuthority("ROLE_USER"))
            val authToken = UsernamePasswordAuthenticationToken(userHeader, null, authorities)
            authToken.details = WebAuthenticationDetailsSource().buildDetails(request)
            SecurityContextHolder.getContext().authentication = authToken
            logger.debug("Аутентифицирован пользователь: $userHeader")
        }
        
        filterChain.doFilter(request, response)
    }
} 