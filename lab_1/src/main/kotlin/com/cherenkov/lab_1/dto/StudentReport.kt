package com.cherenkov.lab_1.dto

data class StudentReport(
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    val groupName: String,
    val departmentName: String,
    val instituteName: String,
    val universityName: String,
    val attendancePercentage: Double,
    val reportPeriod: String,
    val searchTerm: String,
    val redisKey: String?,
    val groupNameFromRedis: String?
)