package com.aegis.query;

/**
 * Represents an aggregation function
 */
public class Aggregation {
    private final String function;
    private final String field;
    private final String alias;

    public Aggregation(String function, String field, String alias) {
        this.function = function;
        this.field = field;
        this.alias = alias;
    }

    public String getFunction() {
        return function;
    }

    public String getField() {
        return field;
    }

    public String getAlias() {
        return alias != null ? alias : function + "_" + field;
    }
}
