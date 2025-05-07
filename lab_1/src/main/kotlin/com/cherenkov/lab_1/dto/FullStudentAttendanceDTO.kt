package com.cherenkov.lab_1.dto

import java.time.LocalDateTime

/**
 * DTO для представления полных данных о посещаемости студента
 */
data class FullStudentAttendanceDTO(
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    val groupName: String,
    val attendancePercent: Double,
    val periodStart: LocalDateTime,
    val periodEnd: LocalDateTime,
    val searchTerm: String
) 