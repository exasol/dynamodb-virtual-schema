package com.exasol.adapter.dynamodb.mapping.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.*;

/**
 * Factory for DynamoDB {@link ValueExtractor}s.
 */
public class DynamodbValueMapperFactory implements AbstractValueMapperFactory<DynamodbNodeVisitor> {

    @Override
    public ValueExtractor<DynamodbNodeVisitor> getValueMapperForColumn(final PropertyToColumnMapping column) {
        final PropertyToColumnVisitor visitor = new PropertyToColumnVisitor();
        column.accept(visitor);
        return visitor.getValueExtractor();
    }

    private static class PropertyToColumnVisitor implements PropertyToColumnMappingVisitor {
        private ValueExtractor<DynamodbNodeVisitor> valueExtractor;

        @Override
        public void visit(final ToStringPropertyToColumnMapping columnDefinition) {
            this.valueExtractor = new DynamodbToStringValueMapper(columnDefinition);
        }

        @Override
        public void visit(final ToJsonPropertyToColumnMapping columnDefinition) {
            this.valueExtractor = new DynamodbToJsonValueMapper(columnDefinition);
        }

        public ValueExtractor<DynamodbNodeVisitor> getValueExtractor() {
            return this.valueExtractor;
        }
    }
}
