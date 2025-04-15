package com.cherenkov.lab_1.dto

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.Instant

data class ReportRequest(
    val term: String,

    @field:JsonFormat(
        shape = JsonFormat.Shape.STRING,
        pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        timezone = "UTC"
    )
    val startDate: Instant,

    @field:JsonFormat(
        shape = JsonFormat.Shape.STRING,
        pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        timezone = "UTC"
    )
    val endDate: Instant
)