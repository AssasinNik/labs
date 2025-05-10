package com.cherenkov.lab_3.configs

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import com.mongodb.client.MongoClients

@Configuration
class MongoConfig {
    
    @Bean
    fun mongoTemplate(): MongoTemplate {
        val connectionString = "mongodb://mongo:27017/university"
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(connectionString), "university")
        return MongoTemplate(factory)
    }
} 