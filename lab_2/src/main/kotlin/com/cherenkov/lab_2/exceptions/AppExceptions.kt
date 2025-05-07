package com.cherenkov.lab_2.exceptions

import java.time.LocalDateTime

/**
 * Базовое исключение приложения
 */
open class AppException(
    val errorCode: String,
    message: String, 
    val details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) : RuntimeException(message, cause)

/**
 * Исключение для ошибок доступа к данным
 */
class DataAccessException(
    message: String, 
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) : AppException("DATA_ACCESS_ERROR", message, details, cause)

/**
 * Исключение для ошибок JWT аутентификации
 */
class JwtAuthenticationException(
    message: String, 
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) : AppException("JWT_AUTH_ERROR", message, details, cause)

/**
 * Исключение для случаев, когда ресурс не найден
 */
class ResourceNotFoundException(
    resourceType: String, 
    resourceId: Any,
    details: Map<String, Any?> = emptyMap()
) : AppException(
    "RESOURCE_NOT_FOUND", 
    "Ресурс типа '$resourceType' с идентификатором '$resourceId' не найден", 
    details
)

/**
 * Исключение для ошибок валидации входных данных
 */
class ValidationException(
    message: String, 
    val invalidFields: Map<String, String> = emptyMap()
) : AppException(
    "VALIDATION_ERROR", 
    message, 
    mapOf("invalidFields" to invalidFields)
)

/**
 * Исключение для ошибок в Neo4j
 */
class Neo4jException(
    message: String, 
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) : AppException("NEO4J_ERROR", message, details, cause)

/**
 * Исключение для ошибок в MongoDB
 */
class MongoDbException(
    message: String, 
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) : AppException("MONGODB_ERROR", message, details, cause)

/**
 * Исключение для ошибок в PostgreSQL
 */
class PostgresException(
    message: String, 
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) : AppException("POSTGRES_ERROR", message, details, cause)

/**
 * Стандартная структура ошибки для ответа клиенту
 */
data class ErrorResponse(
    val timestamp: LocalDateTime = LocalDateTime.now(),
    val status: Int,
    val error: String,
    val code: String,
    val message: String,
    val path: String,
    val details: Map<String, Any?> = emptyMap()
)

/**
 * Исключение для ошибок доступа к Elasticsearch
 */
class ElasticsearchAccessException(
    message: String,
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
):
    AppException("Ошибка доступа к Elasticsearch:", message, details, cause)

/**
 * Исключение для ошибок доступа к Redis
 */
class RedisAccessException(
    message: String,
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) :
    AppException("Ошибка доступа к Redis: ", message, details, cause)

/**
 * Исключение для ошибок доступа к БД
 */
class DatabaseAccessException(
    message: String,
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) :
    AppException("Ошибка доступа к базе данных:", message, details, cause)

/**
 * Исключение для ошибок генерации отчетов
 */
class ReportGenerationException(
    message: String,
    details: Map<String, Any?> = emptyMap(),
    cause: Throwable? = null
) :
    AppException("Ошибка при генерации отчета: ", message, details, cause)