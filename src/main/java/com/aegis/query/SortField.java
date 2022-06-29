package com.aegis.query;

/**
 * Represents a sort field with order
 */
public class SortField {
    private final String field;
    private final String order;

    public SortField(String field, String order) {
        this.field = field;
        this.order = order;
    }

    public String getField() {
        return field;
    }

    public String getOrder() {
        return order;
    }

    public boolean isAscending() {
        return "asc".equalsIgnoreCase(order);
    }
}
