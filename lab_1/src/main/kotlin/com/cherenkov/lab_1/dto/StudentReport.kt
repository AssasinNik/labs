package com.cherenkov.lab_1.dto

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
data class StudentReport(
    // Основные данные студента
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    
    // Учебная информация
    val groupName: String,
    val departmentName: String,
    val instituteName: String,
    val universityName: String,
    
    // Данные о посещаемости
    val attendancePercentage: Double,
    val attendedLectures: Long? = null,
    val totalLectures: Long? = null,
    
    // Информация об отчете
    val reportPeriod: String,
    val searchTerm: String,
    
    // Дополнительные данные из Redis
    val redisKey: String?,
    val groupNameFromRedis: String?,
    
    // Дополнительные данные из MongoDB
    val mongoData: Map<String, Any>? = null
)