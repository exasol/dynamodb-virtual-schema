package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.querypredicate.LogicalOperator;
import com.exasol.adapter.dynamodb.querypredicate.NoPredicate;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;

/**
 * This class represents an OR in the disjunctive normal form (DNF). By the definition of the DNF an OR must only
 * contain ANDs.
 *
 * The DNf structure can be used for query planning.
 *
 * Valid DNF structure examples: {@code (A AND B) OR (C AND !D)} {@code (AND A) OR (AND C)} in this representation the
 * ANDs habe a single operand
 *
 * Invalid DNF: {@code A AND (B OR C)} (AND is not allowed at top level)
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class DnfOr {
    private final Set<DnfAnd> operands;

    /**
     * Create an instance of {@link DnfOr}
     * 
     * @param operands operands of the OR
     */
    public DnfOr(final Set<DnfAnd> operands) {
        this.operands = operands;
    }

    /**
     * Get the operands of this OR
     * 
     * @return operands
     */
    public Set<DnfAnd> getOperands() {
        return this.operands;
    }

    public QueryPredicate asQueryPredicate() {
        final Set<QueryPredicate> convertedOperands = this.operands.stream()
                .map(DnfAnd::asQueryPredicate).filter(dnfAnd -> !(dnfAnd instanceof NoPredicate))
                .collect(Collectors.toSet());
        if (convertedOperands.isEmpty()) {
            return new NoPredicate();
        } else if (convertedOperands.size() == 1) {
            return convertedOperands.iterator().next();
        } else {
            return new LogicalOperator(convertedOperands, LogicalOperator.Operator.OR);
        }
    }

    @Override
    public String toString() {
        final String operatorString = " OR ";
        return "(" + this.operands.stream().map(Object::toString).collect(Collectors.joining(operatorString)) + ")";
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DnfOr)) {
            return false;
        }
        final DnfOr dnfOr = (DnfOr) other;
        return this.operands.equals(dnfOr.operands);
    }

    @Override
    public int hashCode() {
        return this.operands.hashCode();
    }
}
