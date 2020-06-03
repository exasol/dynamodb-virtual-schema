package com.exasol.adapter.dynamodb.remotetablequery;

/**
 * Visitor for {@link QueryPredicate}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface QueryPredicateVisitor<DocumentVisitorType> {
    public void visit(ComparisonPredicate<DocumentVisitorType> comparisonPredicate);

    public void visit(LogicalOperator<DocumentVisitorType> logicalOperator);

    public void visit(NoPredicate<DocumentVisitorType> noPredicate);

    public void visit(NotPredicate<DocumentVisitorType> notPredicate);
}
