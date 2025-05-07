package com.cherenkov.lab_2.dto

/**
 * Запрос для получения отчета о необходимом объеме аудитории для проведения занятий
 */
data class LectureReportRequest(
    val courseId: Long,
    val semester: Int,
    val studyYear: Int
) 