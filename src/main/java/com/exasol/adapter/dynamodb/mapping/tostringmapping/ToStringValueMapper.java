package com.exasol.adapter.dynamodb.mapping.tostringmapping;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.LookupValueMapperException;
import com.exasol.adapter.dynamodb.mapping.ValueMapperException;
import com.exasol.dynamodb.attributevalue.AttributeValueVisitor;
import com.exasol.dynamodb.attributevalue.AttributeValueWrapper;
import com.exasol.dynamodb.attributevalue.UnsupportedDynamodbTypeException;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * ValueMapper for {@link ToStringColumnMappingDefinition}
 */
public class ToStringValueMapper extends AbstractValueMapper {
    private final ToStringColumnMappingDefinition column;

    /**
     * Creates an instance of {@link ToStringColumnMappingDefinition}
     * 
     * @param column {@link ToStringColumnMappingDefinition}
     */
    public ToStringValueMapper(final ToStringColumnMappingDefinition column) {
        super(column);
        this.column = column;
    }

    @Override
    protected ValueExpression mapValue(final AttributeValue dynamodbProperty) throws ValueMapperException {
        final ToStringVisitor toStringVisitor = new ToStringVisitor();
        final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(dynamodbProperty);
        try {
            attributeValueWrapper.accept(toStringVisitor);
        } catch (final UnsupportedDynamodbTypeException exception) {
            throw new LookupValueMapperException(
                    "The DynamoDB type " + exception.getDynamodbTypeName() + " cant't be converted to string.",
                    this.column);
        }
        final String stringValue = toStringVisitor.result;
        if (stringValue == null) {
            return this.column.getDestinationDefaultValue();
        }
        return StringLiteral.of(this.handleOverflow(stringValue));
    }

    private String handleOverflow(final String sourceString) throws OverflowException {
        if (sourceString.length() > this.column.getDestinationStringSize()) {
            if (this.column.getOverflowBehaviour() == ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE) {
                return sourceString.substring(0, this.column.getDestinationStringSize());
            } else {
                throw new OverflowException("String overflow", this.column);
            }
        }
        return sourceString;
    }

    /**
     * Visitor for {@link AttributeValue} that converts its value to string. If this is not possible an
     * {@link UnsupportedOperationException} is thrown.
     */
    private static class ToStringVisitor implements AttributeValueVisitor {
        private String result;

        @Override
        public void visitString(final String value) {
            this.result = value;
        }

        @Override
        public void visitNumber(final String value) {
            this.result = value;
        }

        @Override
        public void visitNull() {
            this.result = null;
        }

        @Override
        public void visitBoolean(final boolean value) {
            this.result = Boolean.TRUE.equals(value) ? "true" : "false";
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
