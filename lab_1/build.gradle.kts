import org.jooq.meta.jaxb.*
import org.jooq.meta.jaxb.Target

plugins {
	kotlin("jvm") version "1.9.25"
	kotlin("plugin.spring") version "1.9.25"
	id("org.springframework.boot") version "3.4.4"
	id("io.spring.dependency-management") version "1.1.7"
	id("nu.studer.jooq") version "9.0"
	kotlin("plugin.serialization") version "1.9.20"
}

group = "com.cherenkov"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(17)
	}
}

repositories {
	mavenCentral()
}

jooq {
	version.set("3.19.21")

	configurations {
		create("main") {  // Конфигурация для основной БД
			generateSchemaSourceOnCompilation.set(true)

			jooqConfiguration.apply {
				jdbc = Jdbc().apply {
					driver = "org.postgresql.Driver"
					url = "jdbc:postgresql://localhost:5433/mydb"
					user = "admin"
					password = "secret"
				}

				generator = Generator().apply {
					name = "org.jooq.codegen.KotlinGenerator"

					database = Database().apply {
						includes = ".*"
						inputSchema = "public"
						excludes = ""
					}

					target = Target().apply {
						packageName = "com.cherenkov.generated.jooq"
						directory = "build/generated-src/jooq/main"
					}

					strategy = Strategy().apply {
						name = "org.jooq.codegen.DefaultGeneratorStrategy"
					}
				}
			}
		}
	}
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-data-mongodb")
	implementation("org.springframework.boot:spring-boot-starter-data-mongodb-reactive")
	implementation("org.springframework.boot:spring-boot-starter-security")
	implementation("org.springframework.security:spring-security-crypto")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("io.projectreactor:reactor-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
	testImplementation("org.springframework.security:spring-security-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

	compileOnly("jakarta.servlet:jakarta.servlet-api:6.1.0")
	implementation("io.jsonwebtoken:jjwt-api:0.12.6")
	runtimeOnly("io.jsonwebtoken:jjwt-impl:0.12.6")
	runtimeOnly("io.jsonwebtoken:jjwt-jackson:0.12.6")

	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")


	// Core
	implementation("org.springframework.boot:spring-boot-starter-webflux")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
	implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")

	// Databases
	implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
	implementation("org.springframework.boot:spring-boot-starter-data-elasticsearch")
	implementation("org.springframework.boot:spring-boot-starter-data-neo4j")
	implementation("org.jooq:jooq-kotlin:3.19.21")
	implementation("org.jooq:jooq:3.19.21")
	implementation("org.jooq:jooq-meta:3.19.21")
	jooqGenerator("org.postgresql:postgresql")
	implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
	runtimeOnly("org.postgresql:r2dbc-postgresql")
	runtimeOnly("org.postgresql:postgresql")
	implementation("org.springframework.boot:spring-boot-starter-jooq")
	implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
	// Observability
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("io.micrometer:micrometer-registry-prometheus")

	// Security
	implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")


	// Test
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("io.mockk:mockk:1.13.8")

	jooqGenerator("org.jooq:jooq-meta-extensions:3.19.21")
	jooqGenerator("org.jooq:jooq-kotlin:3.19.21")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict")
		javaParameters = true
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}

buildscript {
	configurations["classpath"].resolutionStrategy.eachDependency {
		if (requested.group.startsWith("org.jooq") && requested.name.startsWith("jooq")) {
			useVersion("3.19.21")
		}
	}
}

tasks.named<nu.studer.gradle.jooq.JooqGenerate>("generateJooq") { allInputsDeclared.set(true) }
tasks.named<nu.studer.gradle.jooq.JooqGenerate>("generateJooq") {
	(launcher::set)(javaToolchains.launcherFor {
		languageVersion.set(JavaLanguageVersion.of(17))
	})
}