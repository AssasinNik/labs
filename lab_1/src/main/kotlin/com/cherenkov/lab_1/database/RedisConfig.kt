package com.cherenkov.lab_1.database

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

@Configuration
class RedisConfig {

    @Bean
    @Primary
    fun reactiveRedisConnectionFactory(): ReactiveRedisConnectionFactory {
        return LettuceConnectionFactory("localhost", 6379)
    }

    @Bean
    fun reactiveRedisTemplate(
        @Qualifier("reactiveRedisConnectionFactory")
        connectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, Any> {
        val serializer = Jackson2JsonRedisSerializer(Any::class.java)
        val serializationContext = RedisSerializationContext.newSerializationContext<String, Any>()
            .key(StringRedisSerializer())
            .value(serializer)
            .hashKey(StringRedisSerializer())
            .hashValue(serializer)
            .build()

        return ReactiveRedisTemplate(connectionFactory, serializationContext)
    }
}