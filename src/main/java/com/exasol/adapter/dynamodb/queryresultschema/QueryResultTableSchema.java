package com.exasol.adapter.dynamodb.queryresultschema;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

/**
 * Models the result schema of a query
 */
public class QueryResultTableSchema {

    private final TableMappingDefinition fromTable;
    private final List<AbstractColumnMappingDefinition> columns;

    /**
     * Creates an instance of {@link QueryResultTableSchema}.
     *
     * @param fromTable remote table to query
     * @param columns   in correct order
     */
    public QueryResultTableSchema(final TableMappingDefinition fromTable,
            final List<AbstractColumnMappingDefinition> columns) {
        this.fromTable = fromTable;
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

    public TableMappingDefinition getFromTable() {
        return this.fromTable;
    }
}
