package com.cherenkov.gateway.exception

import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException

/**
 * Исключение для ошибок валидации запросов
 */
class ValidationException(message: String) : 
    ResponseStatusException(HttpStatus.BAD_REQUEST, message) 