package com.cherenkov.lab_3.configs

import io.r2dbc.spi.ConnectionFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import javax.sql.DataSource

@Configuration
@EnableTransactionManagement
class TransactionConfig {

    /**
     * Нереактивный транзакционный менеджер для JDBC/JPA операций
     * Помечен как @Primary, так как большинство кода использует нереактивный подход
     */
    @Bean
    @Primary
    fun transactionManager(dataSource: DataSource): PlatformTransactionManager {
        return DataSourceTransactionManager(dataSource)
    }

    /**
     * Реактивный транзакционный менеджер для R2DBC
     * Используется только в реактивных компонентах
     */
    @Bean
    fun reactiveTransactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager {
        return R2dbcTransactionManager(connectionFactory)
    }
} 