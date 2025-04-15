package com.cherenkov.lab_1.dto

data class StudentAttendance(
    val studentNumber: String,
    val total: Long,
    val attended: Long,
    val percentage: Double
)