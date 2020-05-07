package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * ValueMapper for {@link ToJsonColumnMappingDefinition}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class ToJsonValueMapper<DocumentVisitorType> extends AbstractValueMapper<DocumentVisitorType> {

    /**
     * Creates an instance of {@link ToJsonValueMapper}.
     * 
     * @param column {@link ToJsonColumnMappingDefinition}
     */
    public ToJsonValueMapper(final ColumnMappingDefinition column) {
        super(column);
    }

    @Override
    protected ValueExpression mapValue(final DocumentNode<DocumentVisitorType> dynamodbProperty) {
        return StringLiteral.of(mapJsonValue(dynamodbProperty));
    }

    protected abstract String mapJsonValue(final DocumentNode<DocumentVisitorType> dynamodbProperty);
}
