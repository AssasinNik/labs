package com.cherenkov.lab_3.configs

import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.data.neo4j.core.Neo4jTemplate
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories
import org.springframework.transaction.annotation.EnableTransactionManagement

@Configuration
@EnableNeo4jRepositories(basePackages = ["com.cherenkov.lab_3.repository"])
@EnableTransactionManagement
class Neo4jConfig {
    
    @Bean
    fun driver(): Driver {
        return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"))
    }
    
    @Bean
    fun neo4jClient(driver: Driver): Neo4jClient {
        return Neo4jClient.create(driver)
    }
    
    @Bean
    fun neo4jTemplate(client: Neo4jClient): Neo4jTemplate {
        return Neo4jTemplate(client)
    }
} 