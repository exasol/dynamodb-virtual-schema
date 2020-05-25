package com.exasol.adapter.dynamodb.remotetablequery;

/**
 * This class represents a comparison between two values.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class ComparisonPredicate<DocumentVisitorType> implements QueryPredicate<DocumentVisitorType> {
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

    @Override
    public String toString() {
        switch (this.operator) {
        case EQUAL:
            return "=";
        case LESS:
            return "<";
        case LESS_EQUAL:
            return "<=";
        case GREATER:
            return ">";
        case GREATER_EQUAL:
            return ">=";
        default:
            throw new UnsupportedOperationException();// All possible operators are implemented
        }
    }

    /**
     * Possible comparision operators.
     */
    public enum Operator {
        EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL
    }
}
