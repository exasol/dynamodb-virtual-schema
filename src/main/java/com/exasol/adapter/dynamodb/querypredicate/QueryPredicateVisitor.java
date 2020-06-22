package com.exasol.adapter.dynamodb.querypredicate;

/**
 * Visitor for {@link QueryPredicate}.
 */
public interface QueryPredicateVisitor {
    public void visit(ComparisonPredicate comparisonPredicate);

    public void visit(LogicalOperator logicalOperator);

    public void visit(NoPredicate noPredicate);

    public void visit(NotPredicate notPredicate);
}
