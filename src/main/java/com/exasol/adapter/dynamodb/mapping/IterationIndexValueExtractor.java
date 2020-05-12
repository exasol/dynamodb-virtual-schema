package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
import com.exasol.sql.expression.IntegerLiteral;
import com.exasol.sql.expression.ValueExpression;

@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class IterationIndexValueExtractor<DocumentVisitorType> implements ValueExtractor<DocumentVisitorType> {
    private final IterationIndexColumnMapping column;

    public IterationIndexValueExtractor(final IterationIndexColumnMapping column) {
        this.column = column;
    }

    @Override
    public ValueExpression mapRow(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState) {
        final int arrayIndex = arrayAllIterationState.getIndexFor(this.column.getTablesPath());
        return IntegerLiteral.of(arrayIndex);
    }
}
