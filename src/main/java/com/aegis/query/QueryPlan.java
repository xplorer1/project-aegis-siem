package com.aegis.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an execution plan for a multi-tier query
 * Contains sub-queries for each storage tier that needs to be queried
 */
public class QueryPlan {
    private final List<SubQuery> subQueries;

    public QueryPlan() {
        this.subQueries = new ArrayList<>();
    }

    /**
     * Add a sub-query to the execution plan
     */
    public void addSubQuery(SubQuery subQuery) {
        if (subQuery != null) {
            subQueries.add(subQuery);
        }
    }

    /**
     * Get all sub-queries in the plan
     */
    public List<SubQuery> getSubQueries() {
        return subQueries;
    }

    /**
     * Check if the plan has any sub-queries
     */
    public boolean isEmpty() {
        return subQueries.isEmpty();
    }

    /**
     * Get the number of sub-queries
     */
    public int size() {
        return subQueries.size();
    }
}
