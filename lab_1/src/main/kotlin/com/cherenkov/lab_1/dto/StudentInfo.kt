package com.cherenkov.lab_1.dto

data class StudentInfo(
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    val groupName: String,
    val departmentName: String,
    val instituteName: String,
    val universityName: String,
    val redisKey: String?
)