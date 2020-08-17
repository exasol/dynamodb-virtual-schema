package com.exasol.adapter.document.mapping.dynamodb;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.dynamodb.*;
import com.exasol.adapter.document.mapping.PropertyToVarcharColumnMapping;
import com.exasol.adapter.document.mapping.PropertyToVarcharColumnValueExtractor;

/**
 * This class converts DynamoDB values to strings, stored in VARCHAR columns.
 */
public class DynamodbPropertyToVarcharColumnValueExtractor
        extends PropertyToVarcharColumnValueExtractor<DynamodbNodeVisitor> {
    /**
     * Create an instance of {@link DynamodbPropertyToVarcharColumnValueExtractor}.
     *
     * @param column {@link PropertyToVarcharColumnMapping}
     */
    public DynamodbPropertyToVarcharColumnValueExtractor(final PropertyToVarcharColumnMapping column) {
        super(column);
    }

    @Override
    protected String mapStringValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final ToStringVisitor visitor = new ToStringVisitor();
        dynamodbProperty.accept(visitor);
        return visitor.getResult();
    }

    /**
     * Visitor for DynamodbNodes that converts its value to string. If this is not possible an
     * {@link UnsupportedOperationException} is thrown.
     */
    private static class ToStringVisitor implements IncompleteDynamodbNodeVisitor {
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
