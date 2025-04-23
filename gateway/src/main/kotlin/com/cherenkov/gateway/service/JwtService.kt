package com.cherenkov.gateway.service

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
import org.springframework.stereotype.Service
import java.security.Key
import java.util.*
import javax.crypto.SecretKey

@Service
class JwtService {

    private val logger = LoggerFactory.getLogger(JwtService::class.java)

    @Value("\${jwt.secret}")
    private lateinit var secret: String

    @Value("\${jwt.expiration}")
    private var expiration: Long = 0

    private fun getSigningKey(): SecretKey {
        try {
            val keyBytes = Base64.getDecoder().decode(secret)
            return Keys.hmacShaKeyFor(keyBytes)
        } catch (e: Exception) {
            logger.error("Failed to create signing key from secret: ${e.message}", e)
            throw RuntimeException("JWT secret key initialization failed", e)
        }
    }

    fun extractUsername(token: String): String {
        logger.debug("Extracting username from token")
        return extractClaim(token, Claims::getSubject)
    }

    fun isTokenValid(token: String): Boolean {
        try {
            val expiration = extractExpiration(token)
            val isValid = !expiration.before(Date())
            
            if (isValid) {
                logger.debug("Token is valid, expiration: $expiration")
            } else {
                logger.warn("Token has expired, expiration: $expiration, current time: ${Date()}")
            }
            
            return isValid
        } catch (e: Exception) {
            val errorMessage = when (e) {
                is ExpiredJwtException -> "Token expired"
                is SignatureException -> "Invalid JWT signature"
                is MalformedJwtException -> "Malformed JWT"
                is UnsupportedJwtException -> "Unsupported JWT"
                is IllegalArgumentException -> "JWT token compact of handler are invalid"
                else -> "Unexpected error validating token"
            }
            logger.error("$errorMessage: ${e.message}", e)
            return false
        }
    }

    private fun isTokenExpired(token: String): Boolean {
        val expiration = extractExpiration(token)
        val isExpired = expiration.before(Date())
        
        if (isExpired) {
            logger.debug("Token is expired: $expiration")
        }
        
        return isExpired
    }

    private fun extractExpiration(token: String): Date {
        return extractClaim(token, Claims::getExpiration)
    }

    private fun <T> extractClaim(token: String, claimsResolver: (Claims) -> T): T {
        val claims = extractAllClaims(token)
        return claimsResolver(claims)
    }

    private fun extractAllClaims(token: String): Claims {
        try {
            logger.debug("Parsing JWT token")
            val claims = Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .payload
                
            logger.debug("JWT claims extracted successfully: subject=${claims.subject}, expiration=${claims.expiration}")
            return claims
        } catch (e: ExpiredJwtException) {
            logger.warn("JWT token expired: ${e.message}")
            throw e
        } catch (e: SignatureException) {
            logger.error("Invalid JWT signature: ${e.message}")
            throw e
        } catch (e: MalformedJwtException) {
            logger.error("Malformed JWT: ${e.message}")
            throw e
        } catch (e: UnsupportedJwtException) {
            logger.error("Unsupported JWT: ${e.message}")
            throw e
        } catch (e: IllegalArgumentException) {
            logger.error("JWT token compact of handler are invalid: ${e.message}")
            throw e
        } catch (e: JwtException) {
            logger.error("JWT parsing error: ${e.message}")
            throw e
        } catch (e: Exception) {
            logger.error("Unexpected error parsing JWT: ${e.message}", e)
            throw JwtException("Failed to parse JWT token", e)
        }
    }
} 