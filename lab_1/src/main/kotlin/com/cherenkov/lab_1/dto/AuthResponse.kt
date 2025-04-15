package com.cherenkov.lab_1.dto

data class AuthResponse(
    val token: String,
    val refreshToken: String
) 