package com.exasol.adapter.document.querypredicate;

/**
 * This class represents a comparison between two values.
 */
public abstract class AbstractComparisonPredicate implements ComparisonPredicate {
    private static final long serialVersionUID = 3143229347002333048L;
    private final Operator operator;

    /**
     * Create a new instance of {@link AbstractComparisonPredicate}.
     * 
     * @param operator comparison operator
     */
    public AbstractComparisonPredicate(final Operator operator) {
        this.operator = operator;
    }

    @Override
    public Operator getOperator() {
        return this.operator;
    }

    @Override
    public String toString() {
        switch (this.operator) {
        case EQUAL:
            return "=";
        case NOT_EQUAL:
            return "!=";
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

    @Override
    public QueryPredicate simplify() {
        return this;
    }

    protected Operator negateOperator() {
        switch (this.operator) {
        case EQUAL:
            return Operator.NOT_EQUAL;
        case NOT_EQUAL:
            return Operator.EQUAL;
        case LESS:
            return Operator.GREATER_EQUAL;
        case LESS_EQUAL:
            return Operator.GREATER;
        case GREATER:
            return Operator.LESS_EQUAL;
        case GREATER_EQUAL:
            return Operator.LESS;
        default:
            throw new UnsupportedOperationException();// All possible operators are implemented
        }
    }

    /**
     * Possible comparision operators.
     */
    public enum Operator {
        NOT_EQUAL, EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL
    }
}
