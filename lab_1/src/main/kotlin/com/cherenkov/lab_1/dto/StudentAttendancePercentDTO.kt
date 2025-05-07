package com.cherenkov.lab_1.dto

/**
 * DTO для хранения процента посещаемости студента
 */
data class StudentAttendancePercentDTO(
    val studentNumber: String,
    val percent: Double
) 