package com.exasol.adapter.dynamodb.mapping.tostringmapping.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.*;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringValueMapper;

/**
 * This class represents {@link ToStringValueMapper} for DynamoDB values.
 */
public class DynamodbToStringValueMapper extends ToStringValueMapper<DynamodbNodeVisitor> {
    /**
     * Creates an instance of {@link DynamodbToStringValueMapper}.
     *
     * @param column {@link ToStringColumnMappingDefinition}
     */
    public DynamodbToStringValueMapper(final ToStringColumnMappingDefinition column) {
        super(column);
    }

    @Override
    protected String mapStringValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final ToStringVisitor visitor = new ToStringVisitor();
        dynamodbProperty.accept(visitor);
        return visitor.getResult();
    }

    /**
     * Visitor for {@link AttributeValue} that converts its value to string. If this is not possible an
     * {@link UnsupportedOperationException} is thrown.
     */
    private static class ToStringVisitor implements DynamodbNodeVisitor {
        private String result;

        @Override
        public void visit(final DynamodbString string) {
            this.result = string.getValue();
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.result = number.getValue();
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.result = Boolean.TRUE.equals(bool.getValue()) ? "true" : "false";
        }

        @Override
        public void visit(final DynamodbNull nullValue) {
            this.result = null;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new UnsupportedOperationException(
                    "The DynamoDB type " + typeName + " cant't be converted to string. Try using a different mapping.");
        }

        public String getResult() {
            return this.result;
        }
    }
}
