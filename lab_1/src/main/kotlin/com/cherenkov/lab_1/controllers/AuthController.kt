package com.cherenkov.lab_1.controllers

import com.cherenkov.lab_1.security.AuthService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.validation.Valid
import javax.validation.constraints.NotBlank

/**
 * Контроллер для работы с аутентификацией и JWT токенами
 */
@RestController
@RequestMapping("/auth")
@CrossOrigin
class AuthController(
    private val authService: AuthService
) {
    private val logger = LoggerFactory.getLogger(AuthController::class.java)

    /**
     * Запрос на обновление токена
     */
    data class RefreshRequest(
        @field:NotBlank(message = "Refresh токен обязателен")
        val refreshToken: String
    )

    /**
     * Генерирует новую пару токенов (access и refresh)
     */
    @PostMapping("/get_token")
    fun getToken(): ResponseEntity<AuthService.TokenPair> {
        logger.info("Получен запрос на генерацию новых токенов")
        return ResponseEntity.ok(authService.register())
    }

    /**
     * Обновляет токены по refresh токену
     */
    @PostMapping("/refresh")
    fun refresh(
        @Valid @RequestBody body: RefreshRequest
    ): ResponseEntity<AuthService.TokenPair> {
        logger.info("Получен запрос на обновление токенов")
        return try {
            ResponseEntity.ok(authService.refresh(body.refreshToken))
        } catch (e: Exception) {
            logger.error("Ошибка при обновлении токенов: ${e.message}")
            ResponseEntity.status(HttpStatus.UNAUTHORIZED).build()
        }
    }

    /**
     * Обработчик исключений для контроллера
     */
    @ExceptionHandler(Exception::class)
    fun handleException(e: Exception): ResponseEntity<Map<String, String>> {
        logger.error("Ошибка в AuthController: ${e.message}")
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(mapOf("error" to (e.message ?: "Неизвестная ошибка")))
    }
}