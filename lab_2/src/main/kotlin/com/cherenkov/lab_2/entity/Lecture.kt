package com.cherenkov.lab_2.entity

/**
 * Модель лекции
 */
data class Lecture(
    val id: Long,
    val name: String,
    val durationHours: Int,
    val techEquipment: Boolean,
    val courseId: Long,
    val elasticsearchId: String?
) 