package com.cherenkov.lab_3.service

import com.cherenkov.lab_3.dto.User
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Service
class UserService : UserDetailsService {

    // Используем ConcurrentHashMap для потокобезопасности
    private val users = ConcurrentHashMap<String, User>()

    override fun loadUserByUsername(username: String): UserDetails {
        return users[username] ?: throw UsernameNotFoundException("Пользователь не найден")
    }
    
    fun createUser(username: String, password: String, role: String = "ROLE_USER"): User {
        val user = User(
            id = UUID.randomUUID().mostSignificantBits,
            username = username,
            password = "{noop}$password", // {noop} указывает Spring Security не применять шифрование
            role = role
        )
        users[username] = user
        return user
    }
    
    fun userExists(username: String): Boolean {
        return users.containsKey(username)
    }
} 