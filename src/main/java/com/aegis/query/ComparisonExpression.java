package com.aegis.query;

/**
 * Represents a comparison expression (field op value)
 */
public class ComparisonExpression implements Expression {
    private final String field;
    private final String operator;
    private final String value;

    public ComparisonExpression(String field, String operator, String value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }
}
