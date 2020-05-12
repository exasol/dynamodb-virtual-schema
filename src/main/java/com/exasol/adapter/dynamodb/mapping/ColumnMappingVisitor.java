package com.exasol.adapter.dynamodb.mapping;

/**
 * Visitor for {@link ColumnMapping}
 */
public interface ColumnMappingVisitor {
    public void visit(PropertyToColumnMapping propertyToColumnMapping);

    public void visit(IterationIndexColumnMapping iterationIndexColumnDefinition);
}
