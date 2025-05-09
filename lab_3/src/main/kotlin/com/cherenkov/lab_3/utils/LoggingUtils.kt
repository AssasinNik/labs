package com.cherenkov.lab_3.utils

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.security.core.context.SecurityContextHolder
import java.util.*

/**
 * Утилитарный класс для логирования в приложении
 */
object LoggingUtils {
    private val logger = LoggerFactory.getLogger(LoggingUtils::class.java)
    
    /**
     * Добавляет в MDC основные данные для логирования запроса
     */
    fun setupRequestLoggingContext(request: HttpServletRequest) {
        try {
            // Генерация уникального ID для каждого запроса
            val requestId = UUID.randomUUID().toString()
            MDC.put("requestId", requestId)
            
            // Добавление информации о пользователе
            val authentication = SecurityContextHolder.getContext().authentication
            val username = authentication?.name ?: "anonymous"
            MDC.put("username", username)
            
            // Информация о запросе
            MDC.put("clientIp", request.remoteAddr)
            MDC.put("method", request.method)
            MDC.put("uri", request.requestURI)
            
            logger.debug("Настроен контекст логирования для запроса {}: пользователь={}, метод={}, uri={}", 
                         requestId, username, request.method, request.requestURI)
        } catch (e: Exception) {
            logger.error("Ошибка при настройке контекста логирования: {}", e.message, e)
        }
    }
    
    /**
     * Очищает контекст логирования
     */
    fun clearLoggingContext() {
        MDC.clear()
    }
    
    /**
     * Метод для логирования производительности операций
     */
    inline fun <T> logExecutionTime(logger: Logger, operationName: String, block: () -> T): T {
        val startTime = System.currentTimeMillis()
        return try {
            logger.debug("Начало выполнения операции: {}", operationName)
            val result = block()
            val endTime = System.currentTimeMillis()
            logger.debug("Завершение операции: {}, время выполнения: {} мс", 
                        operationName, endTime - startTime)
            result
        } catch (e: Exception) {
            val endTime = System.currentTimeMillis()
            logger.error("Ошибка при выполнении операции: {}, время до ошибки: {} мс, исключение: {}", 
                        operationName, endTime - startTime, e.message, e)
            throw e
        }
    }
    
    /**
     * Форматирует сообщение об исключении с более подробной информацией
     */
    fun formatExceptionMessage(e: Exception): String {
        val stackTrace = e.stackTrace.take(5).joinToString("\n") { " at $it" }
        return "Исключение типа: ${e.javaClass.name}, сообщение: ${e.message}\nСтек вызовов:\n$stackTrace"
    }
} 