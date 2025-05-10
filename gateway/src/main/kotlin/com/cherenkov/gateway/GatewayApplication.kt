package com.cherenkov.gateway

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.cloud.client.discovery.EnableDiscoveryClient
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.context.annotation.Bean
import org.springframework.web.reactive.config.EnableWebFlux

@SpringBootApplication
@EnableWebFlux
@EnableDiscoveryClient
@ConfigurationPropertiesScan
class GatewayApplication {

	@Bean
	fun additionalRoutes(builder: RouteLocatorBuilder): RouteLocator {
		return builder.routes().build()
	}
}

fun main(args: Array<String>) {
	runApplication<GatewayApplication>(*args) {
		setAdditionalProfiles("gateway")
	}
}
