package com.exasol.adapter.dynamodb.queryplan;

/**
 * 
 * @param <DocumentVisitorType>
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
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
