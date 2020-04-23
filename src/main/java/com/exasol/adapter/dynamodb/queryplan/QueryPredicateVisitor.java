package com.exasol.adapter.dynamodb.queryplan;

/**
 * Visitor for {@link DocumentQueryPredicate}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface QueryPredicateVisitor<DocumentVisitorType> {
    public void visit(ColumnLiteralComparisonPredicate<DocumentVisitorType> columnLiteralComparisonPredicate);

    public void visit(AndPredicate<DocumentVisitorType> andPredicate);

    public void visit(OrPredicate<DocumentVisitorType> orPredicate);

    public void visit(NoPredicate<DocumentVisitorType> noPredicate);
}
