plugins {
    java
    id("org.springframework.boot") version "3.2.5"
    id("io.spring.dependency-management") version "1.1.4"
    id("antlr")
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
    
    // OpenSearch dependencies (Task 1.7)
    // OpenSearch REST High Level Client for Hot Tier storage operations
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.12.0")
    
    // OpenSearch Java Client for modern API access
    implementation("org.opensearch.client:opensearch-java:2.8.0")
    
    // ClickHouse dependencies (Task 1.8)
    // ClickHouse JDBC driver for Warm Tier storage operations
    implementation("com.clickhouse:clickhouse-jdbc:0.6.0")
    
    // ClickHouse HTTP client for better performance
    implementation("com.clickhouse:clickhouse-http-client:0.6.0")
    
    // Spring JDBC for JDBC template support
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    
    // Apache Flink dependencies (Task 1.9)
    // Flink Streaming Java for stream processing and correlation engine
    implementation("org.apache.flink:flink-streaming-java:1.18.1")
    
    // Flink Kafka Connector for consuming from Kafka topics
    implementation("org.apache.flink:flink-connector-kafka:3.0.2-1.18")
    
    // Apache Iceberg dependencies (Task 1.10)
    // Iceberg Core for cold storage lakehouse functionality
    implementation("org.apache.iceberg:iceberg-core:1.5.0")
    
    // Iceberg Parquet for columnar storage format
    implementation("org.apache.iceberg:iceberg-parquet:1.5.0")
    
    // Jackson dependencies (Task 1.11)
    // Jackson Databind for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind")
    
    // Jackson Afterburner module for bytecode generation and performance
    implementation("com.fasterxml.jackson.module:jackson-module-afterburner")
    
    // GraphQL dependencies (Task 1.12)
    // Spring Boot GraphQL starter for API Gateway
    implementation("org.springframework.boot:spring-boot-starter-graphql")
    
    // GraphQL Java Extended Scalars for custom scalar types
    implementation("com.graphql-java:graphql-java-extended-scalars:21.0")
    
    // gRPC dependencies (Task 1.13)
    // gRPC Spring Boot Starter for high-performance RPC
    implementation("net.devh:grpc-spring-boot-starter:2.15.0.RELEASE")
    
    // gRPC Protobuf for protocol buffer support
    implementation("io.grpc:grpc-protobuf:1.62.2")
    
    // gRPC Stub for generated service stubs
    implementation("io.grpc:grpc-stub:1.62.2")
    
    // ANTLR4 dependencies (Task 1.14)
    // ANTLR4 Runtime for AQL parser
    antlr("org.antlr:antlr4:4.13.1")
    implementation("org.antlr:antlr4-runtime:4.13.1")
    
    // ONNX Runtime dependencies (Task 1.15)
    // ONNX Runtime for ML model inference in UEBA
    implementation("com.microsoft.onnxruntime:onnxruntime:1.17.1")
    
    // Chronicle Queue dependencies (Task 1.16)
    // Chronicle Queue for reflexive disk buffering
    implementation("net.openhft:chronicle-queue:5.25ea0")
    
    // Chronicle Bytes for low-level byte manipulation
    implementation("net.openhft:chronicle-bytes:2.25ea0")
    
    // Guava and Caffeine cache dependencies (Task 1.17)
    // Guava for utilities including Bloom filters
    implementation("com.google.guava:guava:33.1.0-jre")
    
    // Caffeine for high-performance local caching
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")
    
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
