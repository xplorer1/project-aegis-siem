package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents the result of a query execution across storage tiers.
 * This class encapsulates query results with pagination support.
 */
public class QueryResult {
    
    @JsonProperty("rows")
    private List<Map<String, Object>> rows;
    
    @JsonProperty("total_count")
    private long totalCount;
    
    @JsonProperty("page")
    private int page;
    
    @JsonProperty("page_size")
    private int pageSize;
    
    @JsonProperty("cursor")
    private String cursor; // For cursor-based pagination
    
    @JsonProperty("has_more")
    private boolean hasMore;
    
    @JsonProperty("storage_tier")
    private StorageTier storageTier;
    
    @JsonProperty("execution_time_ms")
    private long executionTimeMs;
    
    /**
     * Default constructor
     */
    public QueryResult() {
        this.rows = new ArrayList<>();
    }
    
    /**
     * Constructor with rows
     */
    public QueryResult(List<Map<String, Object>> rows) {
        this.rows = rows != null ? rows : new ArrayList<>();
        this.totalCount = this.rows.size();
    }
    
    /**
     * Constructor with rows and storage tier
     */
    public QueryResult(List<Map<String, Object>> rows, StorageTier storageTier) {
        this.rows = rows != null ? rows : new ArrayList<>();
        this.totalCount = this.rows.size();
        this.storageTier = storageTier;
    }
    
    // Getters and Setters
    
    public List<Map<String, Object>> getRows() {
        return rows;
    }
    
    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }
    
    public long getTotalCount() {
        return totalCount;
    }
    
    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
    
    public int getPage() {
        return page;
    }
    
    public void setPage(int page) {
        this.page = page;
    }
    
    public int getPageSize() {
        return pageSize;
    }
    
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
    
    public String getCursor() {
        return cursor;
    }
    
    public void setCursor(String cursor) {
        this.cursor = cursor;
    }
    
    public boolean isHasMore() {
        return hasMore;
    }
    
    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }
    
    public StorageTier getStorageTier() {
        return storageTier;
    }
    
    public void setStorageTier(StorageTier storageTier) {
        this.storageTier = storageTier;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    public void setExecutionTimeMs(long executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }
    
    /**
     * Add a row to the result
     */
    public void addRow(Map<String, Object> row) {
        this.rows.add(row);
    }
    
    /**
     * Add all rows from another result
     */
    public void addAll(List<Map<String, Object>> rows) {
        this.rows.addAll(rows);
    }
}
