package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

/**
 * This class represents an OR in the disjunctive normal form (DNF). By the definition of the DNF an OR must only
 * contain ANDs.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class DnfOr<DocumentVisitorType> {
    private final List<DnfAnd<DocumentVisitorType>> operands;

    /**
     * Create an instance of {@link DnfOr}
     * 
     * @param operands operands of the OR
     */
    public DnfOr(final List<DnfAnd<DocumentVisitorType>> operands) {
        this.operands = operands;
    }

    /**
     * Get the operands of this OR
     * 
     * @return operands
     */
    public List<DnfAnd<DocumentVisitorType>> getOperands() {
        return this.operands;
    }

    public QueryPredicate<DocumentVisitorType> asQueryPredicate() {
        if (this.operands.isEmpty()) {
            return new NoPredicate<>();
        } else {
            return new LogicalOperator<>(
                    this.operands.stream().map(DnfAnd::asQueryPredicate).collect(Collectors.toList()),
                    LogicalOperator.Operator.OR);
        }
    }

    @Override
    public String toString() {
        final String operatorString = " OR ";
        return "(" + this.operands.stream().map(Object::toString).collect(Collectors.joining(operatorString)) + ")";
    }
}
