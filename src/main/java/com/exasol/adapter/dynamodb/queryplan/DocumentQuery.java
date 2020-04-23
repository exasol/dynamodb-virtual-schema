package com.exasol.adapter.dynamodb.queryplan;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

/**
 * This class represents the whole query inside of one document.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class DocumentQuery<DocumentVisitorType> implements DocumentQueryMappingInterface {

    private final TableMappingDefinition fromTable;
    private final List<AbstractColumnMappingDefinition> selectList;
    private final DocumentQueryPredicate<DocumentVisitorType> selection;

    /**
     * Creates an instance of {@link DocumentQuery}.
     * 
     * @param fromTable  remote table to query
     * @param selectList in correct order
     * @param selection  where clause
     */
    public DocumentQuery(final TableMappingDefinition fromTable, final List<AbstractColumnMappingDefinition> selectList,
            final DocumentQueryPredicate<DocumentVisitorType> selection) {
        this.fromTable = fromTable;
        this.selectList = selectList;
        this.selection = selection;
    }

    @Override
    public TableMappingDefinition getFromTable() {
        return this.fromTable;
    }

    /**
     * Gives the select list columns.
     * 
     * @return select list columns
     */
    @Override
    public List<AbstractColumnMappingDefinition> getSelectList() {
        return this.selectList;
    }

    /**
     * Gives the where clause of this query.
     * 
     * @return Predicate representing the selection
     */
    public DocumentQueryPredicate<DocumentVisitorType> getSelection() {
        return this.selection;
    }
}
