package com.cherenkov.lab_1.controllers

import com.cherenkov.lab_1.dto.ReportRequest
import com.cherenkov.lab_1.dto.StudentReport
import com.cherenkov.lab_1.service.ReportService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux

@RestController
@RequestMapping("/api/reports")
class ReportController(
    private val reportService: ReportService
) {

    @PostMapping("/attendance")
    fun generateAttendanceReport(@RequestBody request: ReportRequest): Flux<StudentReport> {
        return reportService.generateReport(request)
    }
}