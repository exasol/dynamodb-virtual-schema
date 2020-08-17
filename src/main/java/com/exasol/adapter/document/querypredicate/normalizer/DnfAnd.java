package com.exasol.adapter.document.querypredicate.normalizer;

import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.document.querypredicate.LogicalOperator;
import com.exasol.adapter.document.querypredicate.NoPredicate;
import com.exasol.adapter.document.querypredicate.QueryPredicate;

/**
 * This class represents an AND in the disjunctive normal form (DNF). By the definition of the DNF, an AND is always
 * part of an OR ({@link DnfOr}) and must not contain nested ANDs or ORs.
 */
public final class DnfAnd {
    private final Set<DnfComparison> operands;

    /**
     * Create an instance of {@link DnfAnd}
     * 
     * @param operands operands of this AND
     */
    public DnfAnd(final Set<DnfComparison> operands) {
        this.operands = operands;
    }

    /**
     * Get the operands of this AND.
     * 
     * @return operands
     */
    public Set<DnfComparison> getOperands() {
        return this.operands;
    }

    /**
     * Convert the predicate modeled by this class to an {@link QueryPredicate}.
     *
     * @return {@link QueryPredicate}
     */
    public QueryPredicate asQueryPredicate() {
        final Set<QueryPredicate> convertedOperands = this.operands.stream()
                .map(DnfComparison::asQueryPredicate).filter(comparison -> !(comparison instanceof NoPredicate))
                .collect(Collectors.toSet());
        if (convertedOperands.isEmpty()) {
            return new NoPredicate();
        } else if (convertedOperands.size() == 1) {
            return convertedOperands.iterator().next();
        } else {
            return new LogicalOperator(convertedOperands, LogicalOperator.Operator.AND);
        }
    }

    @Override
    public String toString() {
        final String operatorString = " AND ";
        return "(" + this.operands.stream().map(Object::toString).collect(Collectors.joining(operatorString)) + ")";
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DnfAnd)) {
            return false;
        }
        final DnfAnd dnfAnd = (DnfAnd) other;
        return this.operands.equals(dnfAnd.operands);
    }

    @Override
    public int hashCode() {
        return this.operands.hashCode();
    }
}
