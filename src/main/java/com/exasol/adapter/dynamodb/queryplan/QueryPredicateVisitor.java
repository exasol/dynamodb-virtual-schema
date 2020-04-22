package com.exasol.adapter.dynamodb.queryplan;

public interface QueryPredicateVisitor<DocumentVisitorType> {
    public void visit(ColumnLiteralComparisonPredicate<DocumentVisitorType> columnLiteralComparisonPredicate);

    public void visit(AndPredicate<DocumentVisitorType> andPredicate);

    public void visit(OrPredicate<DocumentVisitorType> orPredicate);
}
