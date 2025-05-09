package com.cherenkov.lab_3.dto

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Запрос для получения отчета по группе студентов
 */
data class GroupReportRequest(
    @JsonProperty("groupId")
    val groupId: Long
) 