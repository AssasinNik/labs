package com.cherenkov.lab_1.configs

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

/**
 * Фильтр для проверки заголовка X-Auth-User, который устанавливается только API Gateway.
 * Этот фильтр гарантирует, что доступ к сервису возможен только через API Gateway,
 * а не напрямую к микросервису.
 */
@Component
@Order(0) // Высокий приоритет, чтобы выполнялся до других фильтров безопасности
class GatewayAuthFilter : OncePerRequestFilter() {

    private val logger = LoggerFactory.getLogger(GatewayAuthFilter::class.java)

    @Value("\${security.gateway.header.name:X-Auth-User}")
    private lateinit var gatewayHeaderName: String

    @Value("\${security.gateway.public-paths:/api/auth/login,/api/auth/register,/api/auth/refresh-token}")
    private lateinit var publicPaths: List<String>

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val requestPath = request.requestURI

        // Пропускаем публичные пути без проверки
        if (isPublicPath(requestPath)) {
            logger.debug("Публичный путь, пропускаем без проверки Gateway: $requestPath")
            filterChain.doFilter(request, response)
            return
        }

        // Получаем заголовок X-Auth-User
        val gatewayAuthHeader = request.getHeader(gatewayHeaderName)

        if (gatewayAuthHeader == null) {
            logger.warn("Попытка прямого доступа к защищенному ресурсу без Gateway: $requestPath")
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Доступ к сервису разрешен только через API Gateway")
            return
        }

        // Если заголовок есть, значит запрос пришел через Gateway - пропускаем дальше
        logger.debug("Запрос от Gateway для пользователя: $gatewayAuthHeader на путь: $requestPath")
        filterChain.doFilter(request, response)
    }

    private fun isPublicPath(path: String): Boolean {
        return publicPaths.any { path.startsWith(it) || path == "/" }
    }
} 