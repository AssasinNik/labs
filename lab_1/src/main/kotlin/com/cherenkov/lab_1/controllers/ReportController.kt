package com.cherenkov.lab_1.controllers

import com.cherenkov.lab_1.dto.ReportRequest
import com.cherenkov.lab_1.dto.ReportResult
import com.cherenkov.lab_1.service.ReportService
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/reports")
class ReportController(
    private val reportService: ReportService
) {
    private val logger = LoggerFactory.getLogger(ReportController::class.java)

    @PostMapping("/attendance")
    fun generateAttendanceReport(@RequestBody request: ReportRequest): ResponseEntity<ReportResult> {
        logger.info("Получен запрос на формирование отчета о посещаемости: term={}, startDate={}, endDate={}", 
                    request.term, request.startDate, request.endDate)
        
        try {
            val result = reportService.generateReport(request)
            
            val statusCode = when (result.status) {
                "SUCCESS" -> {
                    logger.info("Отчет успешно сформирован, записей: {}", result.data.size)
                    org.springframework.http.HttpStatus.OK
                }
                "WARNING" -> {
                    logger.warn("Отчет сформирован с предупреждениями: {}", result.message)
                    org.springframework.http.HttpStatus.OK
                }
                "ERROR" -> {
                    logger.error("Ошибка при формировании отчета: {}", result.message)
                    org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR
                }
                else -> {
                    logger.error("Неизвестный статус отчета: {}", result.status)
                    org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR
                }
            }
            
            return ResponseEntity.status(statusCode).body(result)
        } catch (e: Exception) {
            logger.error("Необработанное исключение при формировании отчета: {}", e.message, e)
            
            val errorResult = ReportResult(
                status = "ERROR",
                message = "Внутренняя ошибка сервера: ${e.message}",
                data = emptyList()
            )
            
            return ResponseEntity.status(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorResult)
        }
    }
}