package com.cherenkov.lab_1.dto

data class StudentReport(
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    val groupName: String,
    val attendancePercentage: Double,
    val reportPeriod: String,
    val searchTerm: String,
)