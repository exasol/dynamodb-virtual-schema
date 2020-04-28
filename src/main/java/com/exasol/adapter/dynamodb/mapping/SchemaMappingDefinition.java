package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

/**
 * Definition of a whole schema (consisting of table definitions) mapping from DynamoDB to Exasol Virtual Schema. An
 * instance of this class represents a whole schema.
 */
public class SchemaMappingDefinition {
    private final List<TableMappingDefinition> tableMappings;

    /**
     * Creates an instance of {@link SchemaMappingDefinition}.
     * 
     * @param tableMappings List of {@link TableMappingDefinition}s
     */
    SchemaMappingDefinition(final List<TableMappingDefinition> tableMappings) {
        this.tableMappings = tableMappings;
    }

    /**
     * Gets the {@link TableMappingDefinition}s
     * 
     * @return List of {@link TableMappingDefinition}s
     */
    public List<TableMappingDefinition> getTableMappings() {
        return this.tableMappings;
    }
}
