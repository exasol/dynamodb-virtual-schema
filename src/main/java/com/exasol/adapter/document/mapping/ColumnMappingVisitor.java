package com.exasol.adapter.document.mapping;

/**
 * Visitor for {@link ColumnMapping}
 */
public interface ColumnMappingVisitor {
    public void visit(PropertyToColumnMapping propertyToColumnMapping);

    public void visit(IterationIndexColumnMapping iterationIndexColumnDefinition);
}
