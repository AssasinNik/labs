package com.cherenkov.lab_1.security

import io.jsonwebtoken.Claims
import io.jsonwebtoken.ExpiredJwtException
import io.jsonwebtoken.JwtException
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.MalformedJwtException
import io.jsonwebtoken.UnsupportedJwtException
import io.jsonwebtoken.security.Keys
import io.jsonwebtoken.security.SignatureException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatusCode
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.util.Base64
import java.util.Date

@Service
class JwtService(
    @Value("\${jwt.secret}") private val jwtSecret: String,
    @Value("\${jwt.access-token-validity-ms:900000}") private val accessTokenValidityMs: Long = 15L * 60L * 1000L,
    @Value("\${jwt.refresh-token-validity-ms:2592000000}") private val refreshTokenValidityMs: Long = 30L * 24 * 60 * 60 * 1000L
) {
    private val logger = LoggerFactory.getLogger(JwtService::class.java)
    private val secretKey = Keys.hmacShaKeyFor(Base64.getDecoder().decode(jwtSecret))

    /**
     * Генерирует JWT токен с указанными параметрами
     */
    private fun generateToken(
        userId: String,
        type: String,
        expiry: Long
    ): String {
        val now = Date()
        val expiryDate = Date(now.time + expiry)
        return Jwts.builder()
            .subject(userId)
            .claim("type", type)
            .issuedAt(now)
            .expiration(expiryDate)
            .signWith(secretKey, Jwts.SIG.HS256)
            .compact()
    }

    /**
     * Генерирует access token для пользователя
     */
    fun generateAccessToken(userId: String): String {
        return generateToken(userId, "access", accessTokenValidityMs)
    }

    /**
     * Генерирует refresh token для пользователя
     */
    fun generateRefreshToken(userId: String): String {
        return generateToken(userId, "refresh", refreshTokenValidityMs)
    }

    /**
     * Проверяет валидность access токена
     */
    fun validateAccessToken(token: String): Boolean {
        try {
            val claims = parseAllClaims(token) ?: return false
            val tokenType = claims["type"] as? String ?: return false
            val isTokenTypeValid = tokenType == "access"
            
            if (!isTokenTypeValid) {
                logger.warn("Неверный тип токена. Ожидался: access, получен: $tokenType")
                return false
            }
            
            // Проверяем срок действия токена
            val expirationDate = claims.expiration
            val now = Date()
            if (expirationDate != null && expirationDate.before(now)) {
                logger.warn("Токен истек. Дата истечения: $expirationDate, текущая дата: $now")
                return false
            }
            
            return true
        } catch (e: Exception) {
            logger.error("Ошибка при валидации access токена: ${e.message}")
            return false
        }
    }

    /**
     * Проверяет валидность refresh токена
     */
    fun validateRefreshToken(token: String): Boolean {
        try {
            val claims = parseAllClaims(token) ?: return false
            val tokenType = claims["type"] as? String ?: return false
            val isTokenTypeValid = tokenType == "refresh"
            
            if (!isTokenTypeValid) {
                logger.warn("Неверный тип токена. Ожидался: refresh, получен: $tokenType")
                return false
            }
            
            // Проверяем срок действия токена
            val expirationDate = claims.expiration
            val now = Date()
            if (expirationDate != null && expirationDate.before(now)) {
                logger.warn("Токен истек. Дата истечения: $expirationDate, текущая дата: $now")
                return false
            }
            
            return true
        } catch (e: Exception) {
            logger.error("Ошибка при валидации refresh токена: ${e.message}")
            return false
        }
    }

    /**
     * Извлекает ID пользователя из токена
     */
    fun getUserIdFromToken(token: String): String {
        val claims = parseAllClaims(token) ?: throw ResponseStatusException(
            HttpStatusCode.valueOf(401),
            "Недействительный токен."
        )
        return claims.subject
    }

    /**
     * Парсит токен и возвращает все его claims
     */
    private fun parseAllClaims(token: String): Claims? {
        val rawToken = if(token.startsWith("Bearer ")) {
            token.removePrefix("Bearer ")
        } else token
        
        return try {
            Jwts.parser()
                .verifyWith(secretKey)
                .build()
                .parseSignedClaims(rawToken)
                .payload
        } catch(e: ExpiredJwtException) {
            logger.warn("Истекший JWT токен: ${e.message}")
            null
        } catch(e: UnsupportedJwtException) {
            logger.error("Неподдерживаемый JWT токен: ${e.message}")
            null
        } catch(e: MalformedJwtException) {
            logger.error("Некорректный JWT токен: ${e.message}")
            null
        } catch(e: SignatureException) {
            logger.error("Недействительная подпись JWT: ${e.message}")
            null
        } catch(e: IllegalArgumentException) {
            logger.error("Пустые JWT claims: ${e.message}")
            null
        } catch(e: JwtException) {
            logger.error("Ошибка JWT: ${e.message}")
            null
        } catch(e: Exception) {
            logger.error("Непредвиденная ошибка при парсинге токена: ${e.message}")
            null
        }
    }
}