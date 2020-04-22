package com.exasol.adapter.dynamodb.queryplan;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;

public class ColumnLiteralComparisonPredicate<DocumentVisitorType> extends ComparisonPredicate<DocumentVisitorType> {
    private final DocumentValue<DocumentVisitorType> literal;
    private final AbstractColumnMappingDefinition column;

    public ColumnLiteralComparisonPredicate(final Operator operator, final AbstractColumnMappingDefinition column,
            final DocumentValue<DocumentVisitorType> literal) {
        super(operator);
        this.literal = literal;
        this.column = column;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }
}
