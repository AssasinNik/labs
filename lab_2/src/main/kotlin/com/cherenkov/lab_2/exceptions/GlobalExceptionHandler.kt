package com.cherenkov.lab_2.exceptions

import io.jsonwebtoken.ExpiredJwtException
import io.jsonwebtoken.JwtException
import io.jsonwebtoken.MalformedJwtException
import io.jsonwebtoken.UnsupportedJwtException
import io.jsonwebtoken.security.SignatureException
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.security.core.AuthenticationException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import java.util.concurrent.TimeoutException

@RestControllerAdvice
class GlobalExceptionHandler {
    private val logger = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)

    /**
     * Обработка кастомных исключений приложения
     */
    @ExceptionHandler(AppException::class)
    fun handleAppException(ex: AppException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Произошла ошибка приложения: {} ({})", ex.message, ex.errorCode, ex)
        
        val status = when (ex) {
            is ResourceNotFoundException -> HttpStatus.NOT_FOUND
            is ValidationException -> HttpStatus.BAD_REQUEST
            is JwtAuthenticationException -> HttpStatus.UNAUTHORIZED
            else -> HttpStatus.INTERNAL_SERVER_ERROR
        }
        
        val errorResponse = ErrorResponse(
            status = status.value(),
            error = status.reasonPhrase,
            code = ex.errorCode,
            message = ex.message ?: "Неизвестная ошибка",
            path = request.requestURI,
            details = ex.details
        )
        
        return ResponseEntity.status(status).body(errorResponse)
    }
    
    /**
     * Обработка ошибок JWT
     */
    @ExceptionHandler(
        value = [
            ExpiredJwtException::class, 
            UnsupportedJwtException::class,
            MalformedJwtException::class,
            SignatureException::class,
            JwtException::class
        ]
    )
    fun handleJwtException(ex: Exception, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Ошибка JWT токена: {}", ex.message, ex)
        
        val message = when (ex) {
            is ExpiredJwtException -> "Срок действия токена истек"
            is UnsupportedJwtException -> "Неподдерживаемый формат токена"
            is MalformedJwtException -> "Некорректный формат токена"
            is SignatureException -> "Недействительная подпись токена"
            else -> "Ошибка проверки токена: ${ex.message}"
        }
        
        val details = mapOf(
            "exceptionType" to ex.javaClass.simpleName,
            "originalMessage" to (ex.message ?: "")
        )
        
        val errorResponse = ErrorResponse(
            status = HttpStatus.UNAUTHORIZED.value(),
            error = HttpStatus.UNAUTHORIZED.reasonPhrase,
            code = "JWT_ERROR",
            message = message,
            path = request.requestURI,
            details = details
        )
        
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorResponse)
    }
    
    /**
     * Обработка ошибок авторизации и аутентификации
     */
    @ExceptionHandler(
        value = [
            AuthenticationException::class,
            AccessDeniedException::class,
            BadCredentialsException::class
        ]
    )
    fun handleAuthenticationException(ex: Exception, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Ошибка авторизации: {}", ex.message, ex)
        
        val status = if (ex is AccessDeniedException) HttpStatus.FORBIDDEN else HttpStatus.UNAUTHORIZED
        
        val errorResponse = ErrorResponse(
            status = status.value(),
            error = status.reasonPhrase,
            code = "AUTHENTICATION_ERROR",
            message = "Ошибка авторизации: ${ex.message}",
            path = request.requestURI
        )
        
        return ResponseEntity.status(status).body(errorResponse)
    }
    
    /**
     * Обработка ошибок валидации аргументов
     */
    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleValidationExceptions(ex: MethodArgumentNotValidException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Ошибка валидации данных: {}", ex.message, ex)
        
        val errors = ex.bindingResult.fieldErrors.associate { it.field to (it.defaultMessage ?: "Некорректное значение") }
        
        val errorResponse = ErrorResponse(
            status = HttpStatus.BAD_REQUEST.value(),
            error = HttpStatus.BAD_REQUEST.reasonPhrase,
            code = "VALIDATION_ERROR",
            message = "Ошибка валидации данных",
            path = request.requestURI,
            details = mapOf("errors" to errors)
        )
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse)
    }
    
    /**
     * Обработка ошибок доступа к данным
     */
    @ExceptionHandler(DataAccessException::class)
    fun handleDataAccessException(ex: DataAccessException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Ошибка доступа к данным: {}", ex.message, ex)
        
        val errorResponse = ErrorResponse(
            status = HttpStatus.INTERNAL_SERVER_ERROR.value(),
            error = HttpStatus.INTERNAL_SERVER_ERROR.reasonPhrase,
            code = "DATABASE_ERROR",
            message = "Ошибка при работе с базой данных",
            path = request.requestURI,
            details = mapOf("cause" to (ex.message ?: ""))
        )
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse)
    }
    
    /**
     * Обработка таймаутов
     */
    @ExceptionHandler(TimeoutException::class)
    fun handleTimeoutException(ex: TimeoutException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Превышено время ожидания: {}", ex.message, ex)
        
        val errorResponse = ErrorResponse(
            status = HttpStatus.GATEWAY_TIMEOUT.value(),
            error = HttpStatus.GATEWAY_TIMEOUT.reasonPhrase,
            code = "TIMEOUT_ERROR",
            message = "Превышено время ожидания ответа",
            path = request.requestURI
        )
        
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body(errorResponse)
    }
    
    /**
     * Обработка всех остальных исключений
     */
    @ExceptionHandler(Exception::class)
    fun handleException(ex: Exception, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        logger.error("Необработанное исключение: {}", ex.message, ex)
        
        val errorResponse = ErrorResponse(
            status = HttpStatus.INTERNAL_SERVER_ERROR.value(),
            error = HttpStatus.INTERNAL_SERVER_ERROR.reasonPhrase,
            code = "INTERNAL_SERVER_ERROR",
            message = "Внутренняя ошибка сервера",
            path = request.requestURI,
            details = mapOf(
                "exceptionType" to ex.javaClass.simpleName,
                "exceptionMessage" to (ex.message ?: "Неизвестная ошибка")
            )
        )
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse)
    }
} 