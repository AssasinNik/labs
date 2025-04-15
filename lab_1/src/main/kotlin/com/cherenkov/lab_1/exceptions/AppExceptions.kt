package com.cherenkov.lab_1.exceptions

/**
 * Базовое исключение приложения
 */
open class AppException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Исключение для ошибок валидации
 */
class ValidationException(message: String) : AppException(message)

/**
 * Исключение для случаев, когда данные не найдены
 */
class ResourceNotFoundException(resourceType: String, id: Any) : 
    AppException("Ресурс типа $resourceType с идентификатором $id не найден")

/**
 * Исключение для ошибок доступа к Elasticsearch
 */
class ElasticsearchAccessException(message: String, cause: Throwable? = null) : 
    AppException("Ошибка доступа к Elasticsearch: $message", cause)

/**
 * Исключение для ошибок доступа к Redis
 */
class RedisAccessException(message: String, cause: Throwable? = null) : 
    AppException("Ошибка доступа к Redis: $message", cause)

/**
 * Исключение для ошибок доступа к БД
 */
class DatabaseAccessException(message: String, cause: Throwable? = null) : 
    AppException("Ошибка доступа к базе данных: $message", cause)

/**
 * Исключение для ошибок генерации отчетов
 */
class ReportGenerationException(message: String, cause: Throwable? = null) : 
    AppException("Ошибка при генерации отчета: $message", cause) 