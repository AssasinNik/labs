package com.cherenkov.lab_3.model

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

/**
 * Документ MongoDB с информацией об университете и его структуре
 */
@Document(collection = "universities")
data class UniversityDocument(
    @Id
    val id: String,
    val name: String,
    val institutes: List<Institute> = emptyList()
)

data class Institute(
    val instituteId: Int = 0,
    val name: String,
    val departments: List<Department> = emptyList()
)

data class Department(
    val departmentId: Int,
    val name: String,
    val specializations: List<String> = emptyList()
) 