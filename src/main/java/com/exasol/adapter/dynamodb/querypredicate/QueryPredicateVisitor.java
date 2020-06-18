package com.exasol.adapter.dynamodb.querypredicate;

/**
 * Visitor for {@link QueryPredicate}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface QueryPredicateVisitor {
    public void visit(ComparisonPredicate comparisonPredicate);

    public void visit(LogicalOperator logicalOperator);

    public void visit(NoPredicate noPredicate);

    public void visit(NotPredicate notPredicate);
}
