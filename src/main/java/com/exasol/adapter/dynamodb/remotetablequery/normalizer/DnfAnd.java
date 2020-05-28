package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

/**
 * This class represents an AND in the disjunctive normal form (DNF). By the definition of the DNF, an AND is always
 * part of an OR ({@link DnfOr}) and must not contain nested ANDs or ORs.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class DnfAnd<DocumentVisitorType> {
    private final List<DnfComparison<DocumentVisitorType>> operands;

    /**
     * Create an instance of {@link DnfAnd}
     * 
     * @param operands operands of this AND
     */
    public DnfAnd(final List<DnfComparison<DocumentVisitorType>> operands) {
        this.operands = operands;
    }

    /**
     * Get the operands of this AND.
     * 
     * @return operands
     */
    public List<DnfComparison<DocumentVisitorType>> getOperands() {
        return this.operands;
    }

    public QueryPredicate<DocumentVisitorType> asQueryPredicate() {
        return new LogicalOperator<>(
                this.operands.stream().map(DnfComparison::asQueryPredicate).collect(Collectors.toList()),
                LogicalOperator.Operator.AND);
    }

    @Override
    public String toString() {
        final String operatorString = " AND ";
        return "(" + this.operands.stream().map(Object::toString).collect(Collectors.joining(operatorString)) + ")";
    }
}
