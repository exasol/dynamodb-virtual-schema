package com.exasol.adapter.dynamodb.documentquery;

/**
 * Visitor for {@link DocumentQueryPredicate}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface QueryPredicateVisitor<DocumentVisitorType> {
    public void visit(ColumnLiteralComparisonPredicate<DocumentVisitorType> columnLiteralComparisonPredicate);

    public void visit(BinaryLogicalOperator<DocumentVisitorType> binaryLogicalOperator);

    public void visit(NoPredicate<DocumentVisitorType> noPredicate);
}
