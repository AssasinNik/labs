package com.cherenkov.lab_1.service

import com.cherenkov.lab_1.dto.AuthRequest
import com.cherenkov.lab_1.dto.AuthResponse
import com.cherenkov.lab_1.dto.RefreshTokenRequest
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.stereotype.Service

@Service
class AuthService(
    private val authenticationManager: AuthenticationManager,
    private val userService: UserService,
    private val jwtService: JwtService
) {

    fun authenticate(request: AuthRequest): AuthResponse {
        val userDetails: UserDetails
        
        // Если пользователь уже существует - используем его, если нет - создаем нового
        if (userService.userExists(request.username)) {
            try {
                // Проверяем учетные данные с помощью AuthenticationManager
                authenticationManager.authenticate(
                    UsernamePasswordAuthenticationToken(
                        request.username,
                        request.password
                    )
                )
                userDetails = userService.loadUserByUsername(request.username)
            } catch (e: Exception) {
                throw IllegalArgumentException("Неверное имя пользователя или пароль")
            }
        } else {
            // Создаем нового пользователя
            userDetails = userService.createUser(request.username, request.password)
        }
        
        val token = jwtService.generateToken(userDetails)
        val refreshToken = jwtService.generateRefreshToken(userDetails)
        
        return AuthResponse(
            token = token,
            refreshToken = refreshToken
        )
    }
    
    fun refreshToken(request: RefreshTokenRequest): AuthResponse {
        val username = jwtService.extractUsername(request.refreshToken)
        
        if (username.isNullOrEmpty() || !userService.userExists(username)) {
            throw IllegalArgumentException("Недействительный токен обновления")
        }
        
        val userDetails = userService.loadUserByUsername(username)
        
        if (!jwtService.isTokenValid(request.refreshToken, userDetails)) {
            throw IllegalArgumentException("Недействительный токен обновления")
        }
        
        val newToken = jwtService.generateToken(userDetails)
        val newRefreshToken = jwtService.generateRefreshToken(userDetails)
        
        return AuthResponse(
            token = newToken,
            refreshToken = newRefreshToken
        )
    }
} 