package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * ValueMapper for {@link ToStringColumnMappingDefinition}
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class ToStringValueMapper<DocumentVisitorType> extends AbstractValueMapper<DocumentVisitorType> {
    private final ToStringColumnMappingDefinition column;

    /**
     * Creates an instance of {@link ToStringValueMapper}.
     * 
     * @param column {@link ToStringColumnMappingDefinition}
     */
    public ToStringValueMapper(final ToStringColumnMappingDefinition column) {
        super(column);
        this.column = column;
    }

    @Override
    protected ValueExpression mapValue(final DocumentNode<DocumentVisitorType> dynamodbProperty) {
        final String stringValue = mapStringValue(dynamodbProperty);
        if (stringValue == null) {
            return this.column.getExasolDefaultValue();
        } else {
            return StringLiteral.of(handleOverflowIfNecessary(stringValue));
        }
    }

    protected abstract String mapStringValue(DocumentNode<DocumentVisitorType> dynamodbProperty);

    private String handleOverflowIfNecessary(final String sourceString) {
        if (sourceString.length() > this.column.getExasolStringSize()) {
            return handleOverflow(sourceString);
        } else {
            return sourceString;
        }
    }

    private String handleOverflow(final String tooLongSourceString) {
        if (this.column.getOverflowBehaviour() == ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE) {
            return tooLongSourceString.substring(0, this.column.getExasolStringSize());
        } else {
            throw new OverflowException(
                    "String overflow. You can either increase the string size if this column or set the overflow behaviour to truncate.",
                    this.column);
        }
    }

    /**
     * Exception thrown if the size of the string from DynamoDB is longer than the configured size.
     */
    public static class OverflowException extends ValueMapperException {
        public OverflowException(final String message, final ToStringColumnMappingDefinition column) {
            super(message, column);
        }
    }
}
