package com.aegis.query;

import java.util.List;

/**
 * Represents a stats clause with aggregations and group by
 */
public class StatsClause {
    private final List<Aggregation> aggregations;
    private final List<String> groupByFields;

    public StatsClause(List<Aggregation> aggregations, List<String> groupByFields) {
        this.aggregations = aggregations;
        this.groupByFields = groupByFields;
    }

    public List<Aggregation> getAggregations() {
        return aggregations;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }
}
