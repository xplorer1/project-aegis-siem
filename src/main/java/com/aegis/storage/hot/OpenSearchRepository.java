package com.aegis.storage.hot;

import com.aegis.domain.OcsfEvent;
import com.aegis.domain.QueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.QueryBuilder;
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
 * Repository for querying OCSF events from OpenSearch
 */
@Repository
public class OpenSearchRepository {
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchRepository.class);
    private static final String INDEX_PATTERN = "aegis-events-*";
    
    @Autowired
    private RestHighLevelClient client;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Search events by query
     * 
     * @param query The query builder
     * @param from Starting offset
     * @param size Number of results
     * @return Query result with events
     */
    public QueryResult<OcsfEvent> search(QueryBuilder query, int from, int size) {
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(query)
                .from(from)
                .size(size)
                .sort("time", SortOrder.DESC);
            
            SearchRequest request = new SearchRequest(INDEX_PATTERN)
                .source(sourceBuilder);
            
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            
            List<OcsfEvent> events = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                OcsfEvent event = objectMapper.readValue(
                    hit.getSourceAsString(), 
                    OcsfEvent.class
                );
                events.add(event);
            }
            
            QueryResult<OcsfEvent> result = new QueryResult<>();
            result.setResults(events);
            result.setTotal(response.getHits().getTotalHits().value);
            result.setTookMs(response.getTook().millis());
            
            logger.debug("Search returned {} results in {} ms", 
                events.size(), response.getTook().millis());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Search failed", e);
            throw new RuntimeException("Search failed", e);
        }
    }
    
    /**
     * Search events by time range
     * 
     * @param startTime Start timestamp (epoch millis)
     * @param endTime End timestamp (epoch millis)
     * @param from Starting offset
     * @param size Number of results
     * @return Query result with events
     */
    public QueryResult<OcsfEvent> searchByTimeRange(
            long startTime, long endTime, int from, int size) {
        
        QueryBuilder query = QueryBuilders.rangeQuery("time")
            .gte(startTime)
            .lte(endTime);
        
        return search(query, from, size);
    }
    
    /**
     * Search events by category
     * 
     * @param category Event category
     * @param from Starting offset
     * @param size Number of results
     * @return Query result with events
     */
    public QueryResult<OcsfEvent> searchByCategory(
            String category, int from, int size) {
        
        QueryBuilder query = QueryBuilders.termQuery("category_name", category);
        return search(query, from, size);
    }
    
    /**
     * Search events by user
     * 
     * @param userId User ID
     * @param from Starting offset
     * @param size Number of results
     * @return Query result with events
     */
    public QueryResult<OcsfEvent> searchByUser(
            String userId, int from, int size) {
        
        QueryBuilder query = QueryBuilders.termQuery("actor.user.uid", userId);
        return search(query, from, size);
    }
    
    /**
     * Search events by source IP
     * 
     * @param ip Source IP address
     * @param from Starting offset
     * @param size Number of results
     * @return Query result with events
     */
    public QueryResult<OcsfEvent> searchBySourceIp(
            String ip, int from, int size) {
        
        QueryBuilder query = QueryBuilders.termQuery("src_endpoint.ip", ip);
        return search(query, from, size);
    }
    
    /**
     * Full-text search across event messages
     * Uses match query with highlighting
     * 
     * @param searchText Text to search for
     * @param from Starting offset
     * @param size Number of results
     * @return Query result with events
     */
    public QueryResult<OcsfEvent> fullTextSearch(
            String searchText, int from, int size) {
        
        try {
            // Build multi-match query across multiple fields
            QueryBuilder query = QueryBuilders.multiMatchQuery(searchText)
                .field("message", 2.0f)  // Boost message field
                .field("category_name")
                .field("class_name")
                .field("actor.user.name")
                .field("src_endpoint.hostname")
                .field("dst_endpoint.hostname")
                .type(org.opensearch.index.query.MultiMatchQueryBuilder.Type.BEST_FIELDS)
                .fuzziness(org.opensearch.common.unit.Fuzziness.AUTO);
            
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(query)
                .from(from)
                .size(size)
                .sort("time", SortOrder.DESC)
                // Add highlighting
                .highlighter(new org.opensearch.search.fetch.subphase.highlight.HighlightBuilder()
                    .field("message")
                    .preTags("<em>")
                    .postTags("</em>")
                    .fragmentSize(150)
                    .numOfFragments(3));
            
            SearchRequest request = new SearchRequest(INDEX_PATTERN)
                .source(sourceBuilder);
            
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            
            List<OcsfEvent> events = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                OcsfEvent event = objectMapper.readValue(
                    hit.getSourceAsString(), 
                    OcsfEvent.class
                );
                
                // Add highlights to metadata
                if (hit.getHighlightFields().containsKey("message")) {
                    var highlights = hit.getHighlightFields().get("message");
                    if (event.getMetadata() == null) {
                        event.setMetadata(new java.util.HashMap<>());
                    }
                    event.getMetadata().put("highlights", 
                        java.util.Arrays.toString(highlights.fragments()));
                }
                
                events.add(event);
            }
            
            QueryResult<OcsfEvent> result = new QueryResult<>();
            result.setResults(events);
            result.setTotal(response.getHits().getTotalHits().value);
            result.setTookMs(response.getTook().millis());
            
            logger.debug("Full-text search for '{}' returned {} results in {} ms", 
                searchText, events.size(), response.getTook().millis());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Full-text search failed", e);
            throw new RuntimeException("Full-text search failed", e);
        }
    }
}
