package com.cherenkov.lab_2.configs

import com.cherenkov.lab_1.utils.LoggingUtils
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingRequestWrapper
import org.springframework.web.util.ContentCachingResponseWrapper
import java.io.UnsupportedEncodingException
import java.nio.charset.StandardCharsets

/**
 * Фильтр для логирования входящих HTTP-запросов и ответов
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class RequestLoggingFilter : OncePerRequestFilter() {
    
    /**
     * Проверка, нужно ли логировать содержимое запроса/ответа
     */
    private fun shouldLogBody(request: HttpServletRequest): Boolean {
        val contentType = request.contentType ?: ""
        return (request.method == "POST" || request.method == "PUT") && 
               (contentType.startsWith("application/json") || 
                contentType.startsWith("application/xml") ||
                contentType.startsWith("text/"))
    }

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        // Установка контекста логирования
        LoggingUtils.setupRequestLoggingContext(request)
        
        val startTime = System.currentTimeMillis()
        
        // Если нужно логировать тело запроса/ответа, оборачиваем в кэширующие обертки
        if (shouldLogBody(request)) {
            val requestWrapper = ContentCachingRequestWrapper(request)
            val responseWrapper = ContentCachingResponseWrapper(response)
            
            try {
                // Логирование запроса
                logger.info(">>> {${request.method}} {${request.requestURI}}")
                
                // Выполнение запроса
                filterChain.doFilter(requestWrapper, responseWrapper)
                
                // Логирование ответа
                val duration = System.currentTimeMillis() - startTime
                logResponse(responseWrapper, duration)
                
                // Копирование контента ответа в оригинальный выходной поток
                responseWrapper.copyBodyToResponse()
            } catch (e: Exception) {
                logger.error("Ошибка при обработке запроса: {${e.message} ${e}}")
                throw e
            } finally {
                LoggingUtils.clearLoggingContext()
            }
        } else {
            try {
                // Логирование запроса без тела
                logger.info(">>> {${request.method}} {${request.requestURI}}")
                
                // Выполнение запроса
                filterChain.doFilter(request, response)
                
                // Логирование ответа
                val duration = System.currentTimeMillis() - startTime
                logger.info("<<< {${request.method}} {${request.requestURI}} - {$response.status} ({$duration} мс)")
            } catch (e: Exception) {
                logger.error("Ошибка при обработке запроса: {${e.message} $e")
                throw e
            } finally {
                LoggingUtils.clearLoggingContext()
            }
        }
    }
    
    /**
     * Логирует детали ответа, в том числе тело, если нужно
     */
    private fun logResponse(response: ContentCachingResponseWrapper, duration: Long) {
        val status = response.status
        val method = response.response
        
        // Логирование основной информации о запросе
        logger.info("<<< {$method} - {$status} ({$duration} мс)")
        
        // Для ошибочных ответов логируем тело
        if (status >= 400) {
            val contentSize = response.contentSize
            if (contentSize > 0) {
                val content = getContentAsString(response.contentAsByteArray, response.characterEncoding)
                if (content.isNotBlank()) {
                    logger.warn("Ответ с ошибкой {$status}: {$content}")
                }
            }
        }
    }
    
    /**
     * Конвертирует массив байтов в строку с указанной кодировкой
     */
    private fun getContentAsString(content: ByteArray, charset: String?): String {
        return try {
            val encoding = charset ?: StandardCharsets.UTF_8.name()
            String(content, charset(encoding))
        } catch (e: UnsupportedEncodingException) {
            logger.warn("Ошибка кодировки при логировании контента: {${e.message}}")
            String(content, StandardCharsets.UTF_8)
        }
    }
} 