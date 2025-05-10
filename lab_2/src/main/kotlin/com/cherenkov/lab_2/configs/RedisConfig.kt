package com.cherenkov.lab_2.configs

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.data.redis.core.RedisTemplate

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

        template.connectionFactory = connectionFactory
        template.keySerializer = strSerializer
        template.valueSerializer = strSerializer
        template.hashKeySerializer = strSerializer
        template.hashValueSerializer = strSerializer
        template.afterPropertiesSet()

        return template
    }
}