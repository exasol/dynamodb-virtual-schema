package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * {@link ColumnValueExtractor} for {@link ToJsonPropertyToColumnMapping}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class ToJsonPropertyToColumnValueExtractor<DocumentVisitorType>
        extends AbstractPropertyToColumnValueExtractor<DocumentVisitorType> {

    /**
     * Create an instance of {@link ToJsonPropertyToColumnValueExtractor}.
     * 
     * @param column {@link ToJsonPropertyToColumnMapping}
     */
    public ToJsonPropertyToColumnValueExtractor(final ToJsonPropertyToColumnMapping column) {
        super(column);
    }

    @Override
    protected ValueExpression mapValue(final DocumentNode<DocumentVisitorType> documentValue) {
        return StringLiteral.of(mapJsonValue(documentValue));
    }

    protected abstract String mapJsonValue(final DocumentNode<DocumentVisitorType> dynamodbProperty);
}
