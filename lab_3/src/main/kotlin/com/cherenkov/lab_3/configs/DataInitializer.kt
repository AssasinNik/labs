package com.cherenkov.lab_3.configs

import com.cherenkov.lab_3.service.UserService
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class DataInitializer(
    private val userService: UserService
) {
    private val logger = LoggerFactory.getLogger(DataInitializer::class.java)

    @PostConstruct
    fun init() {
        logger.info("Инициализация тестовых данных...")
        
        // Создание тестового пользователя, если его еще нет
        if (!userService.userExists("test")) {
            userService.createUser("test", "password")
            logger.info("Создан тестовый пользователь: test")
        }
        
        // Создание пользователя с ролью администратора, если его еще нет
        if (!userService.userExists("admin")) {
            userService.createUser("admin", "admin", "ROLE_ADMIN")
            logger.info("Создан пользователь-администратор: admin")
        }
        
        logger.info("Инициализация тестовых данных завершена")
    }
} 