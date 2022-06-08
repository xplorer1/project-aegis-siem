package com.aegis.storage.hot;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.domain.QueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Repository for querying alerts from OpenSearch
 */
@Repository
public class AlertRepository {
    private static final Logger logger = LoggerFactory.getLogger(AlertRepository.class);
    private static final String ALERTS_INDEX = "aegis-alerts";
    
    @Autowired
    private RestHighLevelClient client;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Find alerts with optional filters
     * 
     * @param severity Optional severity filter (1-5)
     * @param status Optional status filter
     * @param startTime Start timestamp (epoch millis)
     * @param endTime End timestamp (epoch millis)
     * @param limit Maximum number of results
     * @param offset Starting offset for pagination
     * @return List of alerts matching the criteria
     */
    public List<Alert> findAlerts(
            Integer severity,
            AlertStatus status,
            long startTime,
            long endTime,
            int limit,
            int offset) {
        
        try {
            // Build bool query with filters
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            
            // Always filter by time range
            boolQuery.filter(QueryBuilders.rangeQuery("time")
                .gte(startTime)
                .lte(endTime));
            
            // Add severity filter if provided
            if (severity != null) {
                boolQuery.filter(QueryBuilders.termQuery("severity", severity));
            }
            
            // Add status filter if provided
            if (status != null) {
                boolQuery.filter(QueryBuilders.termQuery("status", status.getValue()));
            }
            
            // Build search request
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(boolQuery)
                .from(offset)
                .size(limit)
                .sort("time", SortOrder.DESC);
            
            SearchRequest request = new SearchRequest(ALERTS_INDEX)
                .source(sourceBuilder);
            
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            
            List<Alert> alerts = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Alert alert = objectMapper.readValue(
                    hit.getSourceAsString(), 
                    Alert.class
                );
                alerts.add(alert);
            }
            
            logger.debug("Found {} alerts matching criteria in {} ms", 
                alerts.size(), response.getTook().millis());
            
            return alerts;
            
        } catch (Exception e) {
            logger.error("Failed to query alerts", e);
            throw new RuntimeException("Failed to query alerts", e);
        }
    }
    
    /**
     * Find alert by ID
     * 
     * @param alertId Alert identifier
     * @return Alert if found, null otherwise
     */
    public Alert findById(String alertId) {
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("_id", alertId))
                .size(1);
            
            SearchRequest request = new SearchRequest(ALERTS_INDEX)
                .source(sourceBuilder);
            
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            
            if (response.getHits().getHits().length > 0) {
                SearchHit hit = response.getHits().getHits()[0];
                return objectMapper.readValue(
                    hit.getSourceAsString(), 
                    Alert.class
                );
            }
            
            return null;
            
        } catch (Exception e) {
            logger.error("Failed to find alert by ID: {}", alertId, e);
            throw new RuntimeException("Failed to find alert by ID", e);
        }
    }
    
    /**
     * Count alerts matching criteria
     * 
     * @param severity Optional severity filter
     * @param status Optional status filter
     * @param startTime Start timestamp (epoch millis)
     * @param endTime End timestamp (epoch millis)
     * @return Count of matching alerts
     */
    public long countAlerts(
            Integer severity,
            AlertStatus status,
            long startTime,
            long endTime) {
        
        try {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            
            boolQuery.filter(QueryBuilders.rangeQuery("time")
                .gte(startTime)
                .lte(endTime));
            
            if (severity != null) {
                boolQuery.filter(QueryBuilders.termQuery("severity", severity));
            }
            
            if (status != null) {
                boolQuery.filter(QueryBuilders.termQuery("status", status.getValue()));
            }
            
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(boolQuery)
                .size(0)
                .trackTotalHits(true);
            
            SearchRequest request = new SearchRequest(ALERTS_INDEX)
                .source(sourceBuilder);
            
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            
            return response.getHits().getTotalHits().value;
            
        } catch (Exception e) {
            logger.error("Failed to count alerts", e);
            throw new RuntimeException("Failed to count alerts", e);
        }
    }
    
    /**
     * Save or update an alert
     * 
     * @param alert Alert to save
     * @return Saved alert
     */
    public Alert save(Alert alert) {
        try {
            String alertJson = objectMapper.writeValueAsString(alert);
            
            IndexRequest request = new IndexRequest(ALERTS_INDEX)
                .id(alert.getId())
                .source(alertJson, XContentType.JSON);
            
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            
            logger.debug("Saved alert {} with result: {}", 
                alert.getId(), response.getResult());
            
            return alert;
            
        } catch (Exception e) {
            logger.error("Failed to save alert: {}", alert.getId(), e);
            throw new RuntimeException("Failed to save alert", e);
        }
    }
}
