package com.aegis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for Project AEGIS SIEM.
 * 
 * AEGIS is a next-generation, cloud-native Security Information & Event Management (SIEM) 
 * system designed for high-throughput event ingestion, real-time threat detection, and 
 * multi-petabyte storage capabilities.
 * 
 * Key Features:
 * - 1.5M Events Per Second sustained ingestion
 * - Sub-500ms alert generation latency
 * - Multi-protocol event reception (Syslog, HTTP/HEC, gRPC)
 * - Real-time stream processing with Apache Flink
 * - Tiered storage (Hot/Warm/Cold) for cost-effective retention
 * - Advanced threat detection with Sigma rules and UEBA
 * 
 * @author AEGIS Team
 * @version 1.0.0
 * @since 2022-04-14
 */
@SpringBootApplication
@EnableScheduling
public class AegisApplication {

    /**
     * Main entry point for the AEGIS SIEM application.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(AegisApplication.class, args);
    }
}
