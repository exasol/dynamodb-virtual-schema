package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

/**
 * This interface defines the access to the schema mapping relevant information of a query.
 */
public interface SchemaMappingQuery {

    /**
     * Gives the table defined in the {@code FROM} clause of the statement.
     *
     * @return {{@link TableMappingDefinition}}
     */
    public TableMappingDefinition getFromTable();

    /**
     * Gives the select list columns.
     *
     * @return select list columns
     */
    public List<ColumnMappingDefinition> getSelectList();
}
