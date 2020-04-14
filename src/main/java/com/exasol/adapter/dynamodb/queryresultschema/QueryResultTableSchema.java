package com.exasol.adapter.dynamodb.queryresultschema;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;

/**
 * Models the result schema of a query
 */
public class QueryResultTableSchema {
    private final List<AbstractColumnMappingDefinition> columns;

    /**
     * Creates an instance of {@link QueryResultTableSchema}.
     * 
     * @param columns in correct order
     */
    public QueryResultTableSchema(final List<AbstractColumnMappingDefinition> columns) {
        this.columns = columns;
    }

    /**
     * Get the result columns
     * 
     * @return result columns
     */
    public List<AbstractColumnMappingDefinition> getColumns() {
        return this.columns;
    }
}
