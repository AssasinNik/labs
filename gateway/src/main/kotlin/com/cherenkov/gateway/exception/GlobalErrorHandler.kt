package com.cherenkov.gateway.exception

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler
import org.springframework.core.annotation.Order
import org.springframework.core.io.buffer.DataBufferFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Component
@Order(-2)  // Высокий приоритет для перехвата ошибок
class GlobalErrorHandler(private val objectMapper: ObjectMapper) : ErrorWebExceptionHandler {

    private val logger = LoggerFactory.getLogger(GlobalErrorHandler::class.java)

    override fun handle(exchange: ServerWebExchange, ex: Throwable): Mono<Void> {
        logger.error("Gateway error: ${ex.message}", ex)

        val bufferFactory = exchange.response.bufferFactory()
        val status = determineHttpStatus(ex)
        val errorMessage = determineMessage(ex)

        exchange.response.statusCode = status
        exchange.response.headers.contentType = MediaType.APPLICATION_JSON
        
        val errorResponse = ErrorResponse(
            status = status.value(),
            error = status.reasonPhrase,
            message = errorMessage,
            path = exchange.request.path.toString(),
            timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME)
        )

        val dataBuffer = bufferFactory.wrap(objectMapper.writeValueAsBytes(errorResponse))
        return exchange.response.writeWith(Mono.just(dataBuffer))
    }

    private fun determineHttpStatus(ex: Throwable): HttpStatus {
        return when (ex) {
            is ResponseStatusException -> {
                // Получаем HttpStatus из статус-кода ResponseStatusException
                try {
                    HttpStatus.valueOf(ex.statusCode.value())
                } catch (e: Exception) {
                    HttpStatus.INTERNAL_SERVER_ERROR
                }
            }
            is SecurityException -> HttpStatus.UNAUTHORIZED
            is IllegalArgumentException -> HttpStatus.BAD_REQUEST
            else -> HttpStatus.INTERNAL_SERVER_ERROR
        }
    }

    private fun determineMessage(ex: Throwable): String {
        return when (ex) {
            is ResponseStatusException -> ex.reason ?: ex.message ?: "Unknown error"
            else -> ex.message ?: "Internal Server Error"
        }
    }

    data class ErrorResponse(
        val status: Int,
        val error: String,
        val message: String,
        val path: String,
        val timestamp: String
    )
} 