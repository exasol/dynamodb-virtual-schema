package com.exasol.adapter.dynamodb.mapping.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.*;

/**
 * Factory for DynamoDB {@link ColumnValueExtractor}s.
 */
public class DynamodbPropertyToColumnValueExtractorFactory
        implements PropertyToColumnValueExtractorFactory<DynamodbNodeVisitor> {

    @Override
    public ColumnValueExtractor<DynamodbNodeVisitor> getValueExtractorForColumn(final PropertyToColumnMapping column) {
        final PropertyToColumnVisitor visitor = new PropertyToColumnVisitor();
        column.accept(visitor);
        return visitor.getColumnValueExtractor();
    }

    private static class PropertyToColumnVisitor implements PropertyToColumnMappingVisitor {
        private ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor;

        @Override
        public void visit(final ToStringPropertyToColumnMapping columnDefinition) {
            this.columnValueExtractor = new DynamodbToStringPropertyToColumnValueExtractor(columnDefinition);
        }

        @Override
        public void visit(final ToJsonPropertyToColumnMapping columnDefinition) {
            this.columnValueExtractor = new DynamodbToJsonPropertyToColumnValueExtractor(columnDefinition);
        }

        @Override
        public void visit(final ToDecimalPropertyToColumnMapping columnDefinition) {
            this.columnValueExtractor = new DynamodbToDecimalPropertyToColumnValueExtractor(columnDefinition);
        }

        public ColumnValueExtractor<DynamodbNodeVisitor> getColumnValueExtractor() {
            return this.columnValueExtractor;
        }
    }
}
