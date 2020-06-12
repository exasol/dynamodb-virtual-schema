package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import java.util.Set;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.querypredicate.LogicalOperator;
import com.exasol.adapter.dynamodb.querypredicate.NoPredicate;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;

/**
 * This class represents an AND in the disjunctive normal form (DNF). By the definition of the DNF, an AND is always
 * part of an OR ({@link DnfOr}) and must not contain nested ANDs or ORs.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public final class DnfAnd<DocumentVisitorType> {
    private final Set<DnfComparison<DocumentVisitorType>> operands;

    /**
     * Create an instance of {@link DnfAnd}
     * 
     * @param operands operands of this AND
     */
    public DnfAnd(final Set<DnfComparison<DocumentVisitorType>> operands) {
        this.operands = operands;
    }

    /**
     * Get the operands of this AND.
     * 
     * @return operands
     */
    public Set<DnfComparison<DocumentVisitorType>> getOperands() {
        return this.operands;
    }

    public QueryPredicate<DocumentVisitorType> asQueryPredicate() {
        final Set<QueryPredicate<DocumentVisitorType>> convertedOperands = this.operands.stream()
                .map(DnfComparison::asQueryPredicate).filter(comparison -> !(comparison instanceof NoPredicate))
                .collect(Collectors.toSet());
        if (convertedOperands.isEmpty()) {
            return new NoPredicate<>();
        } else if (convertedOperands.size() == 1) {
            return convertedOperands.iterator().next();
        } else {
            return new LogicalOperator<>(convertedOperands, LogicalOperator.Operator.AND);
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
        final DnfAnd<?> dnfAnd = (DnfAnd<?>) other;
        return this.operands.equals(dnfAnd.operands);
    }

    @Override
    public int hashCode() {
        return this.operands.hashCode();
    }
}
