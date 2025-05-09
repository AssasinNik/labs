package com.cherenkov.lab_3.dto

import com.fasterxml.jackson.annotation.JsonInclude

/**
 * DTO для отчета о посещаемости занятий студентами группы
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class ReportDTO(
    // Информация о группе
    var groupName: String? = null,
    
    // Информация о студенте
    var studentNumber: String? = null,
    var studentName: String? = null,
    var email: String? = null,
    
    // Информация об иерархии университета
    var university: String? = null,
    var institute: String? = null,
    var department: String? = null,
    
    // Информация о курсе
    var courseName: String? = null,
    
    // Информация о часах
    var plannedHours: Int = 0,
    var attendedHours: Int = 0
) 