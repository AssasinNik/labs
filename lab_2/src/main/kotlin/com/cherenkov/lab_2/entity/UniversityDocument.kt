package com.cherenkov.lab_2.entity

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

/**
 * Документ университета в MongoDB
 */
@Document(collection = "universities")
data class UniversityDocument(
    @Id
    val id: String,
    val universityId: Int?,
    val name: String,
    val institutes: List<Institute>
)

/**
 * Институт внутри университета
 */
data class Institute(
    val id: Int?,
    val name: String,
    val departments: List<Department>
)

/**
 * Кафедра внутри института
 */
data class Department(
    val departmentId: Int,
    val name: String
) 