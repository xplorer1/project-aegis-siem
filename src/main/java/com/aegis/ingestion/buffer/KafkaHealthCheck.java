package com.aegis.ingestion.buffer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Health check for Kafka broker availability
 */
@Component
public class KafkaHealthCheck {
    
    private final AdminClient adminClient;
    
    @Autowired
    public KafkaHealthCheck(AdminClient adminClient) {
        this.adminClient = adminClient;
    }
    
    /**
     * Check if Kafka cluster is healthy and available
     */
    public boolean isHealthy() {
        try {
            DescribeClusterResult result = adminClient.describeCluster();
            // Try to get cluster ID with a short timeout
            result.clusterId().get(2, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
