package com.exasol.adapter.dynamodb.queryplan;

public abstract class ComparisonPredicate<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private final Operator operator;

    public ComparisonPredicate(final Operator operator) {
        this.operator = operator;
    }

    public Operator getOperator() {
        return this.operator;
    }

    public enum Operator {
        EQUAL
    }
}
