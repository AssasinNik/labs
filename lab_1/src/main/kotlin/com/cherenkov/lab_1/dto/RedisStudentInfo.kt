package com.cherenkov.lab_1.dto

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * DTO для данных о студенте, полученных из Redis
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class RedisStudentInfo(
    @JsonProperty("fullname")
    val fullname: String,
    
    @JsonProperty("email")
    val email: String,
    
    @JsonProperty("group_id")
    val groupId: Long,
    
    @JsonProperty("group_name")
    val groupName: String,
    
    @JsonProperty("redis_key")
    val redisKey: String
) 