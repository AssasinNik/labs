package com.cherenkov.lab_3.configs

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.serializer.StringRedisSerializer

@Configuration
class RedisConfig {

    @Bean
    @Primary
    fun redisConnectionFactory(): RedisConnectionFactory {
        return LettuceConnectionFactory("redis", 6379)
    }

    @Bean
    fun redisTemplate(redisConnectionFactory: RedisConnectionFactory): RedisTemplate<String, Any> {
        val template = RedisTemplate<String, Any>()
        val strSerializer = StringRedisSerializer()

        template.connectionFactory = redisConnectionFactory
        template.keySerializer = strSerializer
        template.valueSerializer = strSerializer
        template.hashKeySerializer = strSerializer
        template.hashValueSerializer = strSerializer
        template.afterPropertiesSet()

        return template
    }
}