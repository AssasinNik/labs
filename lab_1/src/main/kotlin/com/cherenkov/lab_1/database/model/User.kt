package com.cherenkov.lab_1.database.model

import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document("usersLab")
data class User(
    @Id val id: ObjectId = ObjectId()
)