package com.cherenkov.gateway.service

import org.springframework.cloud.gateway.event.RefreshRoutesEvent
import org.springframework.cloud.gateway.route.RouteDefinition
import org.springframework.cloud.gateway.route.RouteDefinitionWriter
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap

/**
 * Сервис для динамического управления маршрутами API Gateway
 */
@Service
class RouteService(
    private val routeDefinitionWriter: RouteDefinitionWriter,
    private val eventPublisher: ApplicationEventPublisher
) {

    private val routes = ConcurrentHashMap<String, RouteDefinition>()

    /**
     * Добавляет новый маршрут
     */
    fun addRoute(routeDefinition: RouteDefinition): Mono<Void> {
        return Mono.fromCallable {
            routes[routeDefinition.id] = routeDefinition
            routeDefinition
        }
        .flatMap { rd -> routeDefinitionWriter.save(Mono.just(rd)) }
        .doOnSuccess { refreshRoutes() }
        .then()
    }

    /**
     * Обновляет существующий маршрут
     */
    fun updateRoute(routeDefinition: RouteDefinition): Mono<Void> {
        return deleteRoute(routeDefinition.id)
            .then(addRoute(routeDefinition))
    }

    /**
     * Удаляет маршрут
     */
    fun deleteRoute(routeId: String): Mono<Void> {
        return Mono.defer {
            routes.remove(routeId)
            routeDefinitionWriter.delete(Mono.just(routeId))
                .doOnSuccess { refreshRoutes() }
        }
    }

    /**
     * Возвращает список всех маршрутов
     */
    fun getRoutes(): Map<String, RouteDefinition> {
        return routes.toMap()
    }

    /**
     * Получает маршрут по ID
     */
    fun getRoute(routeId: String): RouteDefinition? {
        return routes[routeId]
    }

    /**
     * Обновляет конфигурацию маршрутов в системе
     */
    private fun refreshRoutes() {
        eventPublisher.publishEvent(RefreshRoutesEvent(this))
    }
} 