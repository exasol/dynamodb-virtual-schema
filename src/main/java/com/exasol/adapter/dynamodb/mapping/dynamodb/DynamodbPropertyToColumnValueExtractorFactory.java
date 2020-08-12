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
        public void visit(final PropertyToVarcharColumnMapping columnDefinition) {
            this.columnValueExtractor = new DynamodbPropertyToVarcharColumnValueExtractor(columnDefinition);
        }

        @Override
        public void visit(final PropertyToJsonColumnMapping columnDefinition) {
            this.columnValueExtractor = new DynamodbPropertyToJsonColumnValueExtractor(columnDefinition);
        }

        @Override
        public void visit(final PropertyToDecimalColumnMapping columnDefinition) {
            this.columnValueExtractor = new DynamodbPropertyToDecimalColumnValueExtractor(columnDefinition);
        }

        public ColumnValueExtractor<DynamodbNodeVisitor> getColumnValueExtractor() {
            return this.columnValueExtractor;
        }
    }
}
