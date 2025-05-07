package com.cherenkov.lab_2.controllers

import com.cherenkov.lab_2.dto.LectureReportDTO
import com.cherenkov.lab_2.dto.LectureReportRequest
import com.cherenkov.lab_2.service.ReportService
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

    /**
     * Получение отчета о необходимом объеме аудитории для проведения занятий
     * по курсу заданного семестра и года обучения
     */
    @PostMapping("/course")
    fun getReport(@RequestBody request: LectureReportRequest): ResponseEntity<List<LectureReportDTO>> {
        logger.info("Получен запрос для отчета по курсу: courseId={}, semester={}, studyYear={}", 
            request.courseId, request.semester, request.studyYear)
            
        val report = reportService.generateReport(
            courseId = request.courseId,
            semester = request.semester,
            studyYear = request.studyYear
        )
        
        logger.info("Сформирован отчет, количество записей: {}", report.size)
        return ResponseEntity.ok(report)
    }
}