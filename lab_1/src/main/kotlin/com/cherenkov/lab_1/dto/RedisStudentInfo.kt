package com.cherenkov.lab_1.dto

/**
 * DTO для данных о студенте, полученных из Redis
 */
data class RedisStudentInfo(
    val fullname: String,
    val email: String,
    val groupId: Long,
    val groupName: String
) 