package com.cherenkov.lab_1.database

import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource
import org.springframework.boot.jdbc.DataSourceBuilder

@Configuration
class JooqConfig(private val dataSource: DataSource) {

    @Bean
    fun dslContext(): DSLContext {
        return DSL.using(dataSource, SQLDialect.POSTGRES)
    }
}


