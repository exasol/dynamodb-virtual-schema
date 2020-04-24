package com.exasol.adapter.dynamodb.documentquery;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

/**
 * This interface gives access to the schema mapping information of a {@link DocumentQuery}.
 */
public interface DocumentQueryMappingInterface {

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
    public List<AbstractColumnMappingDefinition> getSelectList();
}