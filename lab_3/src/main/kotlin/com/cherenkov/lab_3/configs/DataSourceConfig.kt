package com.cherenkov.lab_3.configs

import javax.sql.DataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.boot.jdbc.DataSourceBuilder

@Configuration
class DataSourceConfig {

    @Bean
    fun dataSource(): DataSource {
        return DataSourceBuilder.create()
            .url("jdbc:postgresql://postgres:5432/mydb")
            .username("admin")
            .password("secret")
            .build()
    }
}