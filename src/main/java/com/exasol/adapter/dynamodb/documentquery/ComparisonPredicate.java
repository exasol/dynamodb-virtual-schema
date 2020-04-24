package com.exasol.adapter.dynamodb.documentquery;

/**
 * This class represents a comparison between two values.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class ComparisonPredicate<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = 3143229347002333048L;
    private final Operator operator;

    /**
     * Creates a new instance of {@link ComparisonPredicate}.
     * 
     * @param operator comparison operator
     */
    public ComparisonPredicate(final Operator operator) {
        this.operator = operator;
    }

    /**
     * Gives the comparison operator.
     * 
     * @return the operator
     */
    public Operator getOperator() {
        return this.operator;
    }

    /**
     * Possible comparision operators.
     */
    public enum Operator {
        EQUAL
    }
}
