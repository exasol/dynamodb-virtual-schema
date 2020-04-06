package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonValueMapper;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringValueMapper;

/**
 * Factory for {@link AbstractValueMapper}s.
 */
public class ValueMapperFactory {

    /**
     * Builds an ValueMapper fitting to an ColumnMappingDefinition.
     * 
     * @param column ColumnMappingDefinition for which to build the ValueMapper
     * @return built ValueMapper
     */
    public AbstractValueMapper getValueMapperForColumn(final AbstractColumnMappingDefinition column) {
        final ColumnVisitor visitor = new ColumnVisitor();
        column.accept(visitor);
        return visitor.valueMapper;
    }

    private static class ColumnVisitor implements ColumnMappingDefinitionVisitor {
        private AbstractValueMapper valueMapper;

        @Override
        public void visit(final ToStringColumnMappingDefinition columnDefinition) {
            this.valueMapper = new ToStringValueMapper(columnDefinition);
        }

        @Override
        public void visit(final ToJsonColumnMappingDefinition columnDefinition) {
            this.valueMapper = new ToJsonValueMapper(columnDefinition);
        }
    }
}
