package com.cherenkov.lab_3.configs

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.StringRedisSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

@Configuration
class RedisConfig {

    @Bean
    @Primary
    fun redisConnectionFactory(): RedisConnectionFactory {
        return LettuceConnectionFactory("redis", 6379)
    }

    @Bean
    fun objectMapper(): ObjectMapper {
        return ObjectMapper().registerModule(KotlinModule.Builder().build())
    }

    @Bean
    fun redisTemplate(redisConnectionFactory: RedisConnectionFactory, objectMapper: ObjectMapper): RedisTemplate<String, Any> {
        val template = RedisTemplate<String, Any>()
        val jsonSerializer = GenericJackson2JsonRedisSerializer(objectMapper)

        template.connectionFactory = redisConnectionFactory
        template.keySerializer = StringRedisSerializer()
        template.valueSerializer = jsonSerializer
        template.hashKeySerializer = StringRedisSerializer()
        template.hashValueSerializer = jsonSerializer
        template.afterPropertiesSet()

        return template
    }
}