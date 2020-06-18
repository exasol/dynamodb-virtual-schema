package com.exasol.adapter.dynamodb.querypredicate;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents a {@code AND} or {@code OR} logical operator.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public final class LogicalOperator implements QueryPredicate {
    private static final long serialVersionUID = -8351558984178219419L;
    private final Set<QueryPredicate> operands;
    private final Operator operator;

    /**
     * Create new instance of {@link LogicalOperator}.
     *
     * @param operands the operands for this logical operator
     * @param operator logic operator
     */
    public LogicalOperator(final Set<QueryPredicate> operands, final Operator operator) {
        this.operands = operands;
        this.operator = operator;
    }

    /**
     * Get the operands of this logical operator.
     *
     * @return list of operands.
     */
    public Set<QueryPredicate> getOperands() {
        return this.operands;
    }

    /**
     * Get the logic operator.
     *
     * @return the operator
     */
    public Operator getOperator() {
        return this.operator;
    }

    @Override
    public void accept(final QueryPredicateVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public QueryPredicate simplify() {
        final Set<QueryPredicate> simplifiedOperands = this.operands.stream()
                .map(QueryPredicate::simplify).filter(operand -> !(operand instanceof NoPredicate))
                .collect(Collectors.toSet());
        if (simplifiedOperands.isEmpty()) {
            return new NoPredicate();
        } else if (simplifiedOperands.size() == 1) {
            return simplifiedOperands.iterator().next();
        } else {
            return new LogicalOperator(simplifiedOperands, this.operator);
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LogicalOperator)) {
            return false;
        }
        final LogicalOperator that = (LogicalOperator) other;
        return this.operator == that.operator && this.operands.equals(that.operands);
    }

    @Override
    public int hashCode() {
        final int operandsHashCode = this.operands.hashCode();
        return 31 * operandsHashCode + this.operator.hashCode();
    }

    @Override
    public String toString() {
        final String operatorString = this.operator.equals(Operator.AND) ? " AND " : " OR ";
        return "(" + this.operands.stream().map(Object::toString).collect(Collectors.joining(operatorString)) + ")";
    }

    /**
     * Possible operators.
     */
    public enum Operator {
        AND, OR
    }
}
