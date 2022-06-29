package com.aegis.query;

import java.util.List;

/**
 * Represents a sort clause
 */
public class SortClause {
    private final List<SortField> sortFields;

    public SortClause(List<SortField> sortFields) {
        this.sortFields = sortFields;
    }

    public List<SortField> getSortFields() {
        return sortFields;
    }
}
