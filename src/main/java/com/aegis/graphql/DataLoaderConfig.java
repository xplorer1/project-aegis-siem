package com.aegis.graphql;

import com.aegis.domain.OcsfEvent;
import com.aegis.storage.hot.OpenSearchRepository;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.BatchLoaderRegistry;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Configuration for GraphQL DataLoaders.
 * 
 * DataLoaders batch and cache data fetching operations to solve the N+1 query problem.
 * When multiple GraphQL fields request related data, DataLoader collects all the keys
 * and makes a single batch request instead of multiple individual requests.
 * 
 * Key benefits:
 * - Reduces database queries from N+1 to 1 (or a small number of batches)
 * - Improves query performance significantly
 * - Provides automatic caching within a single request
 * 
 * Example scenario:
 * Without DataLoader:
 *   - Query 10 alerts
 *   - Each alert has 5 event IDs
 *   - Total queries: 1 (alerts) + 10 * 5 (events) = 51 queries
 * 
 * With DataLoader:
 *   - Query 10 alerts
 *   - Batch fetch all unique event IDs in 1 query
 *   - Total queries: 1 (alerts) + 1 (events) = 2 queries
 */
@Configuration
public class DataLoaderConfig {
    private static final Logger logger = LoggerFactory.getLogger(DataLoaderConfig.class);
    
    /**
     * Register batch loaders for GraphQL DataLoader.
     * 
     * This method is called by Spring GraphQL to register batch loaders
     * that will be used to create DataLoader instances for each request.
     * 
     * @param registry The batch loader registry
     * @param eventRepository Repository for fetching events
     */
    @Bean
    public BatchLoaderRegistry.RegistrationSpec eventBatchLoader(
            BatchLoaderRegistry registry,
            OpenSearchRepository eventRepository) {
        
        return registry.forTypePair(String.class, OcsfEvent.class)
            .registerBatchLoader((eventIds, batchEnvironment) -> {
                logger.debug("DataLoader batching {} event IDs", eventIds.size());
                
                // Fetch events in a single batch query
                List<OcsfEvent> events = eventRepository.findByIds(eventIds);
                
                logger.debug("DataLoader fetched {} events", events.size());
                
                // Return as CompletableFuture for async processing
                return CompletableFuture.completedFuture(events);
            });
    }
}
