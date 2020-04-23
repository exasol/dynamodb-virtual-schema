package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

/**
 * This interface gives access to the schema mapping information of a {@link DocumentQuery}.
 */
public interface DocumentQueryMappingInterface {

    /**
     * Gives the from table / document of the statement.
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
