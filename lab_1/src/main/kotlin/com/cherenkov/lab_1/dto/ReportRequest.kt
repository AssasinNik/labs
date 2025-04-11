package com.cherenkov.lab_1.dto

import com.fasterxml.jackson.annotation.JsonFormat
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull
import java.time.Instant

data class ReportRequest(
    @field:NotBlank(message = "Поисковый термин должен быть указан")
    val term: String,

    @field:NotNull(message = "Дата начала периода обязательна")
    @field:JsonFormat(
        shape = JsonFormat.Shape.STRING,
        pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        timezone = "UTC"
    )
    val startDate: Instant,

    @field:NotNull(message = "Дата окончания периода обязательна")
    @field:JsonFormat(
        shape = JsonFormat.Shape.STRING,
        pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        timezone = "UTC"
    )
    val endDate: Instant
)