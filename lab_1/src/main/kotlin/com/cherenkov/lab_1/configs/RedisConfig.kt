package com.cherenkov.lab_1.configs

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.core.RedisTemplate
import com.cherenkov.lab_1.dto.RedisStudentInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

@Configuration
class RedisConfig {

    @Bean
    @Primary
    fun redisConnectionFactory(): RedisConnectionFactory {
        return LettuceConnectionFactory("redis", 6379)
    }

    @Bean
    fun redisTemplate(
        @Qualifier("redisConnectionFactory")
        connectionFactory: RedisConnectionFactory
    ): RedisTemplate<String, Any> {
        val template = RedisTemplate<String, Any>()
        val strSerializer = StringRedisSerializer()
        
        // Создаем Jackson сериализатор для JSON
        val jsonSerializer = Jackson2JsonRedisSerializer(RedisStudentInfo::class.java).apply {
            setObjectMapper(ObjectMapper().registerKotlinModule())
        }

        template.connectionFactory = connectionFactory
        template.keySerializer = strSerializer
        template.valueSerializer = jsonSerializer // Используем JSON сериализатор для значений
        template.afterPropertiesSet()

        return template
    }
}