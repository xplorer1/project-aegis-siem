package com.aegis.query;

/**
 * Represents the parsed context of an AQL query
 */
public class QueryContext {
    private String source;
    private TimeRange timeRange;
    private Expression whereExpression;
    private StatsClause statsClause;
    private SortClause sortClause;
    private int limit = -1;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public Expression getWhereExpression() {
        return whereExpression;
    }

    public void setWhereExpression(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    public StatsClause getStatsClause() {
        return statsClause;
    }

    public void setStatsClause(StatsClause statsClause) {
        this.statsClause = statsClause;
    }

    public SortClause getSortClause() {
        return sortClause;
    }

    public void setSortClause(SortClause sortClause) {
        this.sortClause = sortClause;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
