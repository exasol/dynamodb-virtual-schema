package com.exasol.adapter.dynamodb.mapping.tostringmapping;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperException;
import com.exasol.dynamodb.attributevalue.AttributeValueVisitor;
import com.exasol.dynamodb.attributevalue.AttributeValueWrapper;
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
        attributeValueWrapper.accept(toStringVisitor);
        final String stringValue = toStringVisitor.result;
        if (stringValue == null) {
            return this.column.getExasolDefaultValue();
        } else {
            return StringLiteral.of(handleOverflowIfNecessary(stringValue));
        }
    }

    private String handleOverflowIfNecessary(final String sourceString) throws OverflowException {
        if (sourceString.length() > this.column.getExasolStringSize()) {
            return handleOverflow(sourceString);
        } else {
            return sourceString;
        }
    }

    private String handleOverflow(final String tooLongSourceString) throws OverflowException {
        if (this.column.getOverflowBehaviour() == ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE) {
            return tooLongSourceString.substring(0, this.column.getExasolStringSize());
        } else {
            throw new OverflowException(
                    "String overflow. You can either increase the string size if this column or set the overflow behaviour to truncate.",
                    this.column);
        }
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
        public void defaultVisit(final String typeName) {
            throw new UnsupportedOperationException(
                    "The DynamoDB type " + typeName + " cant't be converted to string. Try using a different mapping.");
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
