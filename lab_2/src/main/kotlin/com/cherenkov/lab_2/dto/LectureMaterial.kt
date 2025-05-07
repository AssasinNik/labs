package com.cherenkov.lab_2.dto

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field

@Document(indexName = "lectures")
data class LectureMaterial(
    @Id
    val id: String?,
    @Field("name")
    val name: String,
    @Field("description")
    val description: String,
    @Field("lecture_id")
    val lectureId: Long
)