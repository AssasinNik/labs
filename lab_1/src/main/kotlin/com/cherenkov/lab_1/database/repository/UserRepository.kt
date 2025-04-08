package com.cherenkov.lab_1.database.repository

import com.cherenkov.lab_1.database.model.User
import org.bson.types.ObjectId
import org.springframework.data.mongodb.repository.MongoRepository

interface UserRepository: MongoRepository<User, ObjectId> {
}