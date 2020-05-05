package com.exasol.adapter.dynamodb.remotetablequery;

import java.util.List;

/**
 * This class represents a {@code AND} or {@code OR} logical operator.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class LogicalOperator<DocumentVisitorType> implements QueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = -8351558984178219419L;
    private final List<QueryPredicate<DocumentVisitorType>> operands;
    private final Operator operator;

    /**
     * Creates new instance of {@link LogicalOperator}.
     *
     * @param operands the operands for this logical operator
     * @param operator logic operator
     */
    public LogicalOperator(final List<QueryPredicate<DocumentVisitorType>> operands, final Operator operator) {
        this.operands = operands;
        this.operator = operator;
    }

    /**
     * Gives the operands of this logical operator.
     *
     * @return list of operands.
     */
    public List<QueryPredicate<DocumentVisitorType>> getOperands() {
        return this.operands;
    }

    /**
     * Gives the logic operator.
     *
     * @return the operator
     */
    public Operator getOperator() {
        return this.operator;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final LogicalOperator<?> that = (LogicalOperator<?>) other;
        if (!this.operands.equals(that.operands)) {
            return false;
        }
        return this.operator == that.operator;
    }

    @Override
    public int hashCode() {
        final int operandsHashCode = this.operands.hashCode();
        return 31 * operandsHashCode + this.operator.hashCode();
    }

    /**
     * Possible operators.
     */
    public enum Operator {
        AND, OR
    }
}
