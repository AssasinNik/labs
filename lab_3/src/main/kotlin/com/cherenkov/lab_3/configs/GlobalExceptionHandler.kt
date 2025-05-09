package com.cherenkov.lab_3.configs

import com.cherenkov.lab_3.exceptions.*
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.data.elasticsearch.NoSuchIndexException
import org.springframework.data.elasticsearch.ResourceNotFoundException
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.validation.BindException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.MissingServletRequestParameterException
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler
import java.sql.SQLException
import java.time.format.DateTimeParseException
import javax.naming.ServiceUnavailableException

/**
 * Глобальный обработчик исключений с подробным логированием
 */
@ControllerAdvice
class GlobalExceptionHandler {
    private val logger = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)

    /**
     * Обрабатывает общие исключения при работе с данными
     */
    @ExceptionHandler(DataAccessException::class, SQLException::class, DatabaseAccessException::class)
    fun handleDataAccessException(ex: Exception): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка доступа к базе данных: {}", ex.message, ex)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to "Ошибка базы данных", "message" to "Произошла ошибка при доступе к базе данных"))
    }

    /**
     * Обрабатывает ошибки Elasticsearch
     */
    @ExceptionHandler(NoSuchIndexException::class, ElasticsearchAccessException::class)
    fun handleElasticsearchException(ex: Exception): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка Elasticsearch: {}", ex.message, ex)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to "Ошибка поиска", "message" to "Произошла ошибка при выполнении поиска"))
    }

    /**
     * Обрабатывает ошибки Redis
     */
    @ExceptionHandler(RedisConnectionFailureException::class, RedisAccessException::class)
    fun handleRedisException(ex: Exception): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка Redis: {}", ex.message, ex)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to "Ошибка кэша", "message" to "Произошла ошибка при доступе к кэшу"))
    }

    /**
     * Обрабатывает ошибки валидации входных данных
     */
    @ExceptionHandler(
        MethodArgumentNotValidException::class,
        BindException::class,
        HttpMessageNotReadableException::class,
        MethodArgumentTypeMismatchException::class,
        MissingServletRequestParameterException::class,
        DateTimeParseException::class,
        ValidationException::class
    )
    fun handleValidationExceptions(ex: Exception): ResponseEntity<Map<String, String>> {
        logger.warn("Ошибка валидации входных данных: {}", ex.message, ex)
        
        val errorMessage = when (ex) {
            is MethodArgumentNotValidException -> {
                val errors = ex.bindingResult.fieldErrors.joinToString(", ") { "${it.field}: ${it.defaultMessage}" }
                "Ошибка валидации: $errors"
            }
            is DateTimeParseException -> "Некорректный формат даты: ${ex.message}"
            is HttpMessageNotReadableException -> "Некорректный формат запроса: ${ex.message}"
            is ValidationException -> ex.message ?: "Ошибка валидации данных"
            else -> "Ошибка в параметрах запроса: ${ex.message}"
        }
        
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(mapOf("error" to "Ошибка валидации", "message" to errorMessage))
    }

    /**
     * Обрабатывает случаи, когда ресурс не найден
     */
    @ExceptionHandler(ResourceNotFoundException::class)
    fun handleResourceNotFoundException(ex: ResourceNotFoundException): ResponseEntity<Map<String, String>> {
        logger.warn("Ресурс не найден: {}", ex.message)
        return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(mapOf("error" to "Ресурс не найден", "message" to ex.message!!))
    }

    /**
     * Обрабатывает ошибки при генерации отчетов
     */
    @ExceptionHandler(ReportGenerationException::class)
    fun handleReportGenerationException(ex: ReportGenerationException): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка при генерации отчета: {}", ex.message, ex)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to "Ошибка отчета", "message" to ex.message!!))
    }

    /**
     * Обрабатывает ошибки доступности внешних сервисов
     */
    @ExceptionHandler(ServiceUnavailableException::class)
    fun handleServiceUnavailableException(ex: ServiceUnavailableException): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка доступности сервиса: {}", ex.message, ex)
        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(mapOf("error" to "Сервис недоступен", "message" to "Внешний сервис временно недоступен"))
    }

    /**
     * Обрабатывает специфические для приложения исключения, которые не были обработаны выше
     */
    @ExceptionHandler(AppException::class)
    fun handleAppException(ex: AppException): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка приложения: {}", ex.message, ex)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to "Ошибка приложения", "message" to (ex.message ?: "Произошла ошибка в приложении")))
    }

    /**
     * Обрабатывает все остальные непредвиденные исключения
     */
    @ExceptionHandler(Exception::class)
    fun handleGenericException(ex: Exception): ResponseEntity<Map<String, String>> {
        logger.error("Непредвиденная ошибка: {}", ex.message, ex)
        logger.error("Стек вызовов: ", ex)
        
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to "Внутренняя ошибка сервера", "message" to "Произошла внутренняя ошибка сервера"))
    }
} 