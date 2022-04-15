# Project AEGIS SIEM

## Overview

Project AEGIS is a next-generation, cloud-native Security Information & Event Management (SIEM) system designed to outperform market leaders in throughput, query speed, and detection efficacy. Built as a Security Data Lakehouse, AEGIS decouples compute from storage while maintaining real-time stream processing capabilities for threat detection.

## Key Features

- **High-Throughput Ingestion**: 1.5 Million Events Per Second (EPS) sustained ingestion, 5M EPS burst capacity
- **Real-Time Detection**: Sub-500ms alert generation latency with stateful stream processing
- **Multi-Protocol Support**: Syslog (UDP/TCP), HTTP/HTTPS (HEC-compatible), gRPC
- **Advanced Threat Detection**: Sigma rule engine, UEBA scoring, threat intelligence enrichment
- **Tiered Storage**: Hot (0-7 days), Warm (7-90 days), Cold (90 days - 7 years)
- **High Performance Queries**: Sub-second queries over 10TB datasets
- **Multi-Tenant Architecture**: Strict tenant isolation with dedicated or shared infrastructure
- **Cloud-Native**: Kubernetes-ready, horizontally scalable, highly available

## Architecture

AEGIS follows a Kappa Architecture pattern with the following components:

- **Titan Ingestion Layer**: Reactive, non-blocking event reception
- **Prism Normalization Engine**: 500+ vendor log format parsers with OCSF schema mapping
- **Sentinel Correlation Engine**: Apache Flink-based stream processing for threat detection
- **Vault Storage System**: Tiered storage with OpenSearch (Hot), ClickHouse (Warm), Apache Iceberg (Cold)
- **API Gateway**: GraphQL, gRPC, and REST APIs with AQL query language

## Technology Stack

- **Language**: Java 17 (LTS)
- **Framework**: Spring Boot 3.2+
- **Build Tool**: Gradle 8+ with Kotlin DSL
- **Streaming**: Apache Kafka / Apache Pulsar
- **Stream Processing**: Apache Flink
- **Hot Storage**: OpenSearch 2.x
- **Warm Storage**: ClickHouse
- **Cold Storage**: Apache Iceberg on S3-compatible object storage
- **State Management**: Redis Cluster
- **Service Mesh**: Istio / Linkerd

## Prerequisites

- Java 17 or higher
- Gradle 8.0 or higher
- Docker and Docker Compose (for local development)
- Kafka cluster (or use Docker Compose)
- Redis instance (or use Docker Compose)

## Building the Project

### Build with Gradle

```bash
# Build the project
./gradlew build

# Build without tests
./gradlew build -x test

# Run tests
./gradlew test

# Create executable JAR
./gradlew bootJar
```

### Build Output

The build produces an executable JAR file in `build/libs/`:
- `aegis-siem-<version>.jar` - Executable Spring Boot application

## Running the Application

### Development Mode

```bash
# Run with development profile
./gradlew bootRun --args='--spring.profiles.active=dev'

# Or run the JAR directly
java -jar build/libs/aegis-siem-*.jar --spring.profiles.active=dev
```

### Production Mode

```bash
# Run with production profile
java -jar build/libs/aegis-siem-*.jar --spring.profiles.active=prod
```

### Using Docker Compose (Local Development)

```bash
# Start all dependencies (Kafka, Redis, OpenSearch, ClickHouse)
docker-compose up -d

# Run the application
./gradlew bootRun --args='--spring.profiles.active=dev'
```

## Configuration

### Application Profiles

- **default**: Base configuration (application.yml)
- **dev**: Development profile with debug logging (application-dev.yml)
- **prod**: Production profile with optimized settings (application-prod.yml)

### Environment Variables (Production)

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TRUSTSTORE_LOCATION=/path/to/truststore.jks
KAFKA_TRUSTSTORE_PASSWORD=changeit

# Redis
REDIS_HOST=redis-cluster.example.com
REDIS_PORT=6379
REDIS_PASSWORD=changeit

# OpenSearch
OPENSEARCH_HOSTS=opensearch1:9200,opensearch2:9200
OPENSEARCH_USERNAME=admin
OPENSEARCH_PASSWORD=changeit

# ClickHouse
CLICKHOUSE_URL=jdbc:clickhouse://clickhouse:8123/aegis
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=changeit
```

## API Endpoints

### Health Check
```
GET http://localhost:8080/actuator/health
```

### Metrics (Prometheus)
```
GET http://localhost:8080/actuator/prometheus
```

### Event Ingestion (HEC-compatible)
```
POST http://localhost:8080/services/collector/event
Authorization: Splunk <token>
Content-Type: application/json

{
  "event": {
    "message": "User login successful",
    "severity": "info"
  }
}
```

## Development

### Project Structure

```
project-aegis-siem/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/aegis/
│   │   │       ├── AegisApplication.java
│   │   │       ├── ingestion/          # Titan Ingestion Layer
│   │   │       ├── normalization/      # Prism Normalization Engine
│   │   │       ├── correlation/        # Sentinel Correlation Engine
│   │   │       ├── storage/            # Vault Storage System
│   │   │       └── api/                # API Gateway
│   │   └── resources/
│   │       ├── application.yml
│   │       ├── application-dev.yml
│   │       └── application-prod.yml
│   └── test/
│       ├── java/
│       └── resources/
├── build.gradle.kts
├── settings.gradle.kts
└── README.md
```

### Code Style

- Follow Java naming conventions
- Use meaningful variable and method names
- Add JavaDoc comments for public APIs
- Keep methods focused and concise
- Write unit tests for all business logic

## Testing

### Run All Tests
```bash
./gradlew test
```

### Run Specific Test Class
```bash
./gradlew test --tests com.aegis.ingestion.SyslogUdpListenerTest
```

### Integration Tests
```bash
./gradlew integrationTest
```

## Performance Targets

- **Ingestion**: 1.5M EPS sustained, 5M EPS burst
- **Alert Latency**: < 500ms P99
- **Query Latency**: < 1s for 1B records (aggregations)
- **Storage**: Multi-petabyte capacity
- **Availability**: 99.99% uptime

## Monitoring

### Metrics

AEGIS exposes Prometheus metrics at `/actuator/prometheus`:

- Ingestion rate (events/sec)
- Parser throughput and failure rates
- Alert generation latency
- Query execution times
- Storage tier utilization

### Distributed Tracing

OpenTelemetry traces are exported to configured OTLP endpoint for end-to-end request tracking.

## Contributing

1. Create a feature branch from `main`
2. Implement your changes with tests
3. Ensure all tests pass: `./gradlew test`
4. Submit a pull request with clear description

## License

Copyright © 2022 AEGIS Team. All rights reserved.

## Support

For issues and questions, please contact the AEGIS development team.
