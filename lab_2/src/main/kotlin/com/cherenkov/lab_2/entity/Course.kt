package com.cherenkov.lab_2.entity

/**
 * Модель курса обучения
 */
data class Course(
    val id: Long,
    val name: String,
    val departmentId: Long
) 