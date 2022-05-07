package com.aegis.query;

import com.aegis.domain.StorageTier;

/**
 * ClickHouse-specific sub-query implementation
 * Wraps a SQL string for warm tier queries
 */
public class ClickHouseQuery implements SubQuery {
    
    private final String sql;

    public ClickHouseQuery(String sql) {
        this.sql = sql;
    }

    @Override
    public StorageTier getTier() {
        return StorageTier.WARM;
    }

    @Override
    public Object getNativeQuery() {
        return sql;
    }

    /**
     * Get the SQL string for execution
     */
    public String getSql() {
        return sql;
    }
}
