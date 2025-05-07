package com.cherenkov.gateway.controller

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.time.LocalDateTime

@RestController
@RequestMapping("/fallback")
class FallbackController {

    private val logger = LoggerFactory.getLogger(FallbackController::class.java)

    /**
     * Общий обработчик для всех fallback запросов
     */
    @GetMapping("/**")
    fun handleGetFallback(): Mono<FallbackResponse> {
        return createFallbackResponse("Service is temporarily unavailable. Please try again later.")
    }

    /**
     * Обработчик для lab1 сервиса
     */
    @GetMapping("/lab1")
    fun handleLab1Fallback(): Mono<FallbackResponse> {
        logger.warn("Fallback triggered for Lab1 service")
        return createFallbackResponse("Lab1 service is temporarily unavailable. Please try again later.")
    }

    /**
     * Обработчик для lab2 сервиса
     */
    @GetMapping("/lab2")
    fun handleLab2Fallback(): Mono<FallbackResponse> {
        logger.warn("Fallback triggered for Lab2 service")
        return createFallbackResponse("Lab2 service is temporarily unavailable. Please try again later.")
    }

    /**
     * Fallback для POST запросов
     */
    @PostMapping("/**")
    fun handlePostFallback(): Mono<FallbackResponse> {
        return createFallbackResponse("Service is temporarily unavailable. Your request could not be processed.")
    }

    /**
     * Fallback для PUT запросов
     */
    @PutMapping("/**")
    fun handlePutFallback(): Mono<FallbackResponse> {
        return createFallbackResponse("Service is temporarily unavailable. Your update could not be processed.")
    }

    /**
     * Fallback для DELETE запросов
     */
    @DeleteMapping("/**")
    fun handleDeleteFallback(): Mono<FallbackResponse> {
        return createFallbackResponse("Service is temporarily unavailable. Your delete request could not be processed.")
    }

    private fun createFallbackResponse(message: String): Mono<FallbackResponse> {
        return Mono.just(
            FallbackResponse(
                status = HttpStatus.SERVICE_UNAVAILABLE.value(),
                error = "Service Unavailable",
                message = message,
                timestamp = LocalDateTime.now().toString()
            )
        )
    }

    /**
     * Структура для ответа fallback
     */
    data class FallbackResponse(
        val status: Int,
        val error: String,
        val message: String,
        val timestamp: String
    )
} 