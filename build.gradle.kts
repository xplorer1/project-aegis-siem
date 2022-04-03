plugins {
    java
    id("org.springframework.boot") version "3.2.5"
    id("io.spring.dependency-management") version "1.1.4"
}

group = "com.aegis"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

// Spring Boot BOM (Bill of Materials) for dependency management
dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
    }
}

dependencies {
    // Spring WebFlux for reactive non-blocking I/O (Task 1.3)
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    
    // Reactor Netty for high-performance network handling
    implementation("io.projectreactor.netty:reactor-netty")
    
    // Spring MVC for control plane APIs (Task 1.4)
    // Note: This creates a dual web stack - WebFlux for ingestion, MVC for control plane
    implementation("org.springframework.boot:spring-boot-starter-web")
    
    // Kafka client dependencies (Task 1.5)
    // Spring Kafka provides Spring integration with Kafka
    implementation("org.springframework.kafka:spring-kafka")
    
    // Kafka clients library for producer/consumer APIs
    implementation("org.apache.kafka:kafka-clients")
    
    // Redis dependencies (Task 1.6)
    // Spring Data Redis for Redis integration and state management
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    
    // Lettuce Core for reactive Redis client
    implementation("io.lettuce:lettuce-core")
    
    // Spring Boot dependencies will be added in subsequent tasks
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
