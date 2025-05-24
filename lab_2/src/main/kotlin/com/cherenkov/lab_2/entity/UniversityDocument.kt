package com.cherenkov.lab_2.entity

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field

/**
 * Документ MongoDB с информацией об университете и его структуре
 */
@Document(collection = "universities")
data class UniversityDocument(
    @Id
    val id: Int,
    
    @Field("name")
    val name: String,
    
    @Field("institutes")
    val institutes: List<Institute>
)

data class Institute(
    @Field("_id")
    val id: Int,
    
    @Field("name")
    val name: String,
    
    @Field("departments")
    val departments: List<Department>
)

data class Department(
    @Field("_id")
    val id: Int,
    
    @Field("name")
    val name: String
) 