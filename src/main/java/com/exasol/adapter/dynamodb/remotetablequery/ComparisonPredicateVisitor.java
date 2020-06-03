package com.exasol.adapter.dynamodb.remotetablequery;

/**
 * This is an interface for visitors for {@link ComparisonPredicate}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface ComparisonPredicateVisitor<DocumentVisitorType> {
    public void visit(final ColumnLiteralComparisonPredicate<DocumentVisitorType> columnLiteralComparisonPredicate);
}
