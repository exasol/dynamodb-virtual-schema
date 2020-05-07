package com.exasol.adapter.dynamodb.mapping;

/**
 * Visitor for {@link ColumnMappingDefinition}.
 */
public interface ColumnMappingDefinitionVisitor {
    /**
     * Visits an {@link ToStringColumnMappingDefinition}.
     * 
     * @param columnDefinition {@link ToStringColumnMappingDefinition} to visit
     */
    public void visit(ToStringColumnMappingDefinition columnDefinition);

    /**
     * Visits an {@link ToJsonColumnMappingDefinition}.
     * 
     * @param columnDefinition {@link ToJsonColumnMappingDefinition} to visit
     */
    public void visit(ToJsonColumnMappingDefinition columnDefinition);
}
