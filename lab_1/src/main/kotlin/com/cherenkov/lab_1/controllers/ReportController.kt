package com.cherenkov.lab_1.controllers

import com.cherenkov.lab_1.dto.ReportRequest
import com.cherenkov.lab_1.dto.StudentReport
import com.cherenkov.lab_1.service.ReportService
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.validation.Valid
import org.springframework.web.bind.annotation.CrossOrigin

/**
 * Контроллер для генерации отчетов о посещаемости студентов
 */
@RestController
@RequestMapping("/api/reports")
@CrossOrigin
class ReportController(
    private val reportService: ReportService
) {
    private val logger = LoggerFactory.getLogger(ReportController::class.java)

    /**
     * Генерирует отчет о студентах с минимальным процентом посещения лекций,
     * содержащих указанный термин, за определенный период
     */
    @PostMapping("/attendance")
    @PreAuthorize("isAuthenticated()")
    fun generateAttendanceReport(@Valid @RequestBody request: ReportRequest): ResponseEntity<List<StudentReport>> {
        logger.info("Получен запрос на отчет о посещаемости для термина: ${request.term}")
        
        val report = reportService.generateReport(request)
        
        return if (report.isEmpty()) {
            logger.warn("Отчет о посещаемости для термина '${request.term}' не содержит данных")
            ResponseEntity.noContent().build()
        } else {
            logger.info("Сформирован отчет о посещаемости (${report.size} записей)")
            ResponseEntity.ok(report)
        }
    }
}