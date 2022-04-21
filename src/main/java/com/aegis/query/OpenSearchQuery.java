package com.aegis.query;

import com.aegis.domain.StorageTier;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * OpenSearch-specific sub-query implementation
 * Wraps a SearchSourceBuilder for hot tier queries
 */
public class OpenSearchQuery implements SubQuery {
    
    private final SearchSourceBuilder searchSourceBuilder;

    public OpenSearchQuery(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
    }

    @Override
    public StorageTier getTier() {
        return StorageTier.HOT;
    }

    @Override
    public Object getNativeQuery() {
        return searchSourceBuilder;
    }

    /**
     * Get the SearchSourceBuilder for execution
     */
    public SearchSourceBuilder getSearchSourceBuilder() {
        return searchSourceBuilder;
    }
}
