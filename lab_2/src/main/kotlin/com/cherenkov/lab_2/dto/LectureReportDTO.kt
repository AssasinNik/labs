package com.cherenkov.lab_2.dto

/**
 * DTO для отчета о необходимом объеме аудитории для проведения занятий
 */
data class LectureReportDTO(
    val courseName: String,
    val lectureId: Long,
    val lectureName: String,
    val techEquipment: Boolean,
    val studentCount: Long,
    val universityName: String,
    val instituteName: String,
    val departmentName: String,
    val semester: Int,
    val studyYear: Int
) 