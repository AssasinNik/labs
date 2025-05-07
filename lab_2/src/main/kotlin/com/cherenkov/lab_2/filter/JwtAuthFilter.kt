package com.cherenkov.lab_2.filter
import com.cherenkov.lab_2.service.JwtService
import io.jsonwebtoken.ExpiredJwtException
import io.jsonwebtoken.JwtException
import io.jsonwebtoken.MalformedJwtException
import io.jsonwebtoken.UnsupportedJwtException
import io.jsonwebtoken.security.SignatureException
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.User
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class JwtAuthFilter(
    private val jwtService: JwtService,
    @Value("\${security.gateway.header.name:X-Auth-User}")
    private val gatewayHeaderName: String,
    @Value("\${gateway.auth.header-name:X-Gateway-Auth}")
    private val gatewayAuthHeaderName: String,
    @Value("\${gateway.auth.header-value:true}")
    private val gatewayAuthHeaderValue: String
) : OncePerRequestFilter() {

    private val logger: Log = LogFactory.getLog(JwtAuthFilter::class.java)

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val requestId = generateRequestId()
        val requestUri = request.requestURI
        val method = request.method
        
        logger.debug("[$requestId] Начало проверки JWT для запроса $method $requestUri")
        
        try {
            // Игнорируем пути, не требующие проверки JWT
            if (shouldSkipAuth(requestUri)) {
                logger.debug("[$requestId] Пропуск проверки JWT для публичного пути: $requestUri")
                filterChain.doFilter(request, response)
                return
            }
            
            // Проверка заголовка от gateway
            val gatewayAuthHeader = request.getHeader(gatewayAuthHeaderName)
            if (gatewayAuthHeader == null || gatewayAuthHeader != gatewayAuthHeaderValue) {
                logger.warn("[$requestId] Отсутствует или некорректный заголовок шлюза '$gatewayAuthHeaderName': $gatewayAuthHeader")
                response.status = HttpServletResponse.SC_UNAUTHORIZED
                response.contentType = "application/json"
                response.writer.write("""
                    {
                        "status": 401,
                        "error": "Unauthorized",
                        "code": "GATEWAY_AUTH_ERROR",
                        "message": "Доступ запрещен. Запрос должен проходить через Gateway.",
                        "path": "$requestUri"
                    }
                """.trimIndent())
                return
            }
            
            // Проверка заголовка с JWT токеном
            val authHeader = request.getHeader("Authorization")
            val headerInfo = if (authHeader != null) "Present (starts with ${authHeader.take(15)}...)" else "Missing"
            logger.debug("[$requestId] Authorization Header: $headerInfo")
                
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                logger.warn("[$requestId] Отсутствует или неверный формат JWT токена")
                filterChain.doFilter(request, response)
                return
            }
            
            val jwt = authHeader.substring(7)
            logger.debug("[$requestId] Извлечен JWT токен, начинается с: ${jwt.take(10)}...")
            
            // Извлечение и проверка username из токена
            val username = extractUsername(jwt, requestId)
            logger.debug("[$requestId] Извлечен username из токена: $username")
            
            // Проверка контекста безопасности
            if (username.isNotEmpty() && SecurityContextHolder.getContext().authentication == null) {
                logger.debug("[$requestId] Создание UserDetails для пользователя: $username")
                val userDetails = createUserDetails(username)
                
                if (jwtService.isTokenValid(jwt, userDetails)) {
                    logger.debug("[$requestId] JWT токен действителен, создание аутентификации")
                    val authToken = UsernamePasswordAuthenticationToken(
                        userDetails, null, userDetails.authorities
                    )
                    authToken.details = WebAuthenticationDetailsSource().buildDetails(request)
                    SecurityContextHolder.getContext().authentication = authToken
                    logger.info("[$requestId] Пользователь '$username' успешно аутентифицирован")
                } else {
                    logger.warn("[$requestId] JWT токен недействителен для пользователя: $username")
                }
            }
        } catch (e: ExpiredJwtException) {
            logger.warn("[$requestId] JWT токен истек: ${e.message}")
        } catch (e: UnsupportedJwtException) {
            logger.warn("[$requestId] Неподдерживаемый JWT токен: ${e.message}")
        } catch (e: MalformedJwtException) {
            logger.warn("[$requestId] Некорректный формат JWT токена: ${e.message}")
        } catch (e: SignatureException) {
            logger.warn("[$requestId] Недействительная подпись JWT токена: ${e.message}")
        } catch (e: JwtException) {
            logger.warn("[$requestId] Ошибка обработки JWT токена: ${e.message}")
        } catch (e: Exception) {
            logger.error("[$requestId] Необработанное исключение при проверке JWT: ${e.message}", e)
        } finally {
            logger.debug("[$requestId] Завершение проверки JWT, продолжение цепочки фильтров")
            filterChain.doFilter(request, response)
        }
    }
    
    /**
     * Извлечение имени пользователя из токена с обработкой ошибок
     */
    private fun extractUsername(jwt: String, requestId: String): String {
        return try {
            jwtService.extractUsername(jwt)
        } catch (e: Exception) {
            logger.warn("[$requestId] Ошибка при извлечении имени пользователя из токена: ${e.message}")
            ""
        }
    }
    
    /**
     * Проверка, нужно ли пропустить аутентификацию для данного пути
     */
    private fun shouldSkipAuth(requestUri: String): Boolean {
        val publicPaths = listOf(
            "/actuator", 
            "/swagger-ui", 
            "/api-docs", 
            "/v3/api-docs"
        )
        
        return publicPaths.any { requestUri.startsWith(it) }
    }
    
    /**
     * Создание пользовательских данных на основе имени пользователя
     */
    private fun createUserDetails(username: String): UserDetails {
        return User.builder()
            .username(username)
            .password("")
            .roles("USER")
            .build()
    }
    
    /**
     * Генерация уникального ID запроса для связывания логов
     */
    private fun generateRequestId(): String {
        return java.util.UUID.randomUUID().toString().substring(0, 8)
    }
} 