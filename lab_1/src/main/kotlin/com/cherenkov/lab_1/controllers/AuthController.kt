package com.cherenkov.lab_1.controllers

import com.cherenkov.lab_1.security.AuthService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/auth")
class AuthController(
    private val authService: AuthService
) {
    data class RefreshRequest(
        val refreshToken: String
    )

    @PostMapping("/get_token")
    fun getToken(): AuthService.TokenPair {
        return authService.register()
    }

    @PostMapping("/refresh")
    fun refresh(
        @RequestBody body: RefreshRequest
    ): AuthService.TokenPair {
        return authService.refresh(body.refreshToken)
    }
}