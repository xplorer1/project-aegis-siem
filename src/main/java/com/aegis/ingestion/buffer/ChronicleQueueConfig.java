package com.aegis.ingestion.buffer;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycles;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;

/**
 * Configuration for Chronicle Queue disk buffering
 */
@Configuration
public class ChronicleQueueConfig {
    
    @Value("${aegis.buffer.queue-directory:./data/buffer-queue}")
    private String queueDirectory;
    
    @Value("${aegis.buffer.roll-cycle:HOURLY}")
    private String rollCycle;
    
    /**
     * Create Chronicle Queue bean for disk buffering
     */
    @Bean
    public ChronicleQueue chronicleQueue() {
        RollCycles cycle = RollCycles.valueOf(rollCycle);
        
        return ChronicleQueue.singleBuilder()
                .path(Paths.get(queueDirectory))
                .rollCycle(cycle)
                .build();
    }
}
