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
    protected MappedStringResult mapStringValue(final DocumentNode<DynamodbNodeVisitor> dynamodbProperty) {
        final ToStringVisitor visitor = new ToStringVisitor();
        dynamodbProperty.accept(visitor);
        return visitor.getResult();
    }

    /**
     * Visitor for DynamodbNodes that converts its value to string. If this is not possible an
     * {@link UnsupportedOperationException} is thrown.
     */
    private static class ToStringVisitor implements IncompleteDynamodbNodeVisitor {
        private MappedStringResult result;

        @Override
        public void visit(final DynamodbString string) {
            this.result = new MappedStringResult(string.getValue(), false);
        }

        @Override
        public void visit(final DynamodbNumber number) {
            this.result = new MappedStringResult(number.getValue(), true);
        }

        @Override
        public void visit(final DynamodbBoolean bool) {
            this.result = new MappedStringResult(Boolean.TRUE.equals(bool.getValue()) ? "true" : "false", true);
        }

        @Override
        public void visit(final DynamodbNull nullValue) {
            this.result = new MappedStringResult(null, false);
        }

        @Override
        public void defaultVisit(final String typeName) {
            this.result = null;
        }

        public MappedStringResult getResult() {
            return this.result;
        }
    }
}
