package com.cherenkov.gateway.controller

import com.cherenkov.gateway.service.RouteService
import org.springframework.cloud.gateway.route.RouteDefinition
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.net.URI

/**
 * Контроллер для управления маршрутами API Gateway (административный интерфейс)
 * Требует административных прав для доступа
 */
@RestController
@RequestMapping("/admin/routes")
@PreAuthorize("hasRole('ADMIN')")
class RoutesManagementController(private val routeService: RouteService) {

    /**
     * Получить все маршруты
     */
    @GetMapping
    fun getRoutes(): ResponseEntity<Map<String, RouteDefinition>> {
        return ResponseEntity.ok(routeService.getRoutes())
    }

    /**
     * Получить маршрут по ID
     */
    @GetMapping("/{id}")
    fun getRoute(@PathVariable id: String): ResponseEntity<RouteDefinition> {
        val route = routeService.getRoute(id)
        return if (route != null) {
            ResponseEntity.ok(route)
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * Добавить новый маршрут
     */
    @PostMapping
    fun addRoute(@RequestBody route: RouteDefinition): Mono<ResponseEntity<Void>> {
        return routeService.addRoute(route)
            .thenReturn(ResponseEntity.created(URI.create("/admin/routes/${route.id}")).build())
    }

    /**
     * Обновить существующий маршрут
     */
    @PutMapping("/{id}")
    fun updateRoute(@PathVariable id: String, @RequestBody route: RouteDefinition): Mono<ResponseEntity<Void>> {
        if (id != route.id) {
            return Mono.just(ResponseEntity.badRequest().build())
        }
        
        return routeService.updateRoute(route)
            .thenReturn(ResponseEntity.ok().build())
    }

    /**
     * Удалить маршрут
     */
    @DeleteMapping("/{id}")
    fun deleteRoute(@PathVariable id: String): Mono<ResponseEntity<Void>> {
        return routeService.deleteRoute(id)
            .thenReturn(ResponseEntity.ok().build())
    }
} 