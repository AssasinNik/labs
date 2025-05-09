package com.cherenkov.lab_3.controllers

import com.cherenkov.lab_3.dto.GroupReportRequest
import com.cherenkov.lab_3.dto.ReportDTO
import com.cherenkov.lab_3.service.ReportService
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/reports")
class ReportController(private val reportService: ReportService) {
    private val log = LoggerFactory.getLogger(ReportController::class.java)

    /**
     * Получить отчет по группе студентов
     * @param request Запрос с ID группы
     * @return Список отчетов по студентам группы
     */
    @PostMapping("/group")
    fun getReportByGroup(@RequestBody request: GroupReportRequest): ResponseEntity<List<ReportDTO>> {
        log.info("Получен запрос на формирование отчета по группе: {}", request.groupId)
        val result = reportService.getReportByGroup(request.groupId)
        return ResponseEntity.ok(result)
    }
}