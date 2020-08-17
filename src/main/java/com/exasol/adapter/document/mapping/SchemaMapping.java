package com.exasol.adapter.document.mapping;

import java.util.List;

/**
 * Definition of a schema, consisting of table definitions, mapping from DynamoDB to Exasol Virtual Schema. An instance
 * of this class represents a whole schema.
 */
public class SchemaMapping {
    private final List<TableMapping> tableMappings;

    /**
     * Create an instance of {@link SchemaMapping}.
     * 
     * @param tableMappings List of {@link TableMapping}s
     */
    public SchemaMapping(final List<TableMapping> tableMappings) {
        this.tableMappings = tableMappings;
    }

    /**
     * Gets the {@link TableMapping}s
     * 
     * @return List of {@link TableMapping}s
     */
    public List<TableMapping> getTableMappings() {
        return this.tableMappings;
    }
}
