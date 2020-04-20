package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.dynamodb.DynamodbToJsonValueMapper;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.dynamodb.DynamodbToStringValueMapper;

/**
 * Factory for DynamoDB {@link AbstractValueMapper}s.
 */
public class DynamodbValueMapperFactory implements ValueMapperFactory<DynamodbNodeVisitor> {

    @Override
    public AbstractValueMapper<DynamodbNodeVisitor> getValueMapperForColumn(
            final AbstractColumnMappingDefinition column) {
        final ColumnVisitor visitor = new ColumnVisitor();
        column.accept(visitor);
        return visitor.getValueMapper();
    }

    private static class ColumnVisitor implements ColumnMappingDefinitionVisitor {
        private AbstractValueMapper<DynamodbNodeVisitor> valueMapper;

        @Override
        public void visit(final ToStringColumnMappingDefinition columnDefinition) {
            this.valueMapper = new DynamodbToStringValueMapper(columnDefinition);
        }

        @Override
        public void visit(final ToJsonColumnMappingDefinition columnDefinition) {
            this.valueMapper = new DynamodbToJsonValueMapper(columnDefinition);
        }

        public AbstractValueMapper<DynamodbNodeVisitor> getValueMapper() {
            return this.valueMapper;
        }
    }
}