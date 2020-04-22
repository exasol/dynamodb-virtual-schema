package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

/**
 * This class represents the whole query inside of one document.
 */
public class DocumentQuery {

    private final TableMappingDefinition fromTable;
    private final List<AbstractColumnMappingDefinition> selectList;

    /**
     * Creates an instance of {@link DocumentQuery}.
     *
     * @param fromTable  remote table to query
     * @param selectList in correct order
     */
    public DocumentQuery(final TableMappingDefinition fromTable,
            final List<AbstractColumnMappingDefinition> selectList) {
        this.fromTable = fromTable;
        this.selectList = selectList;
    }

    /**
     * Get the select list columns.
     * 
     * @return select list columns
     */
    public List<AbstractColumnMappingDefinition> getSelectList() {
        return this.selectList;
    }

    public TableMappingDefinition getFromTable() {
        return this.fromTable;
    }
}
