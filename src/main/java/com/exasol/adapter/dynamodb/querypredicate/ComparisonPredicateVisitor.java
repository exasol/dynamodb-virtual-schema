package com.exasol.adapter.dynamodb.querypredicate;

/**
 * This is an interface for visitors for {@link ComparisonPredicate}s.
 */
public interface ComparisonPredicateVisitor {
    public void visit(final ColumnLiteralComparisonPredicate columnLiteralComparisonPredicate);
}
