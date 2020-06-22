package com.exasol.adapter.dynamodb.querypredicate;

/**
 * This is an interface for visitors for {@link ComparisonPredicate}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface ComparisonPredicateVisitor {
    public void visit(final ColumnLiteralComparisonPredicate columnLiteralComparisonPredicate);
}