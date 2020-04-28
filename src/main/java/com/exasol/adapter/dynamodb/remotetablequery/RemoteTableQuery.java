package com.exasol.adapter.dynamodb.remotetablequery;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingQuery;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;

/**
 * This class represents the whole query inside of one document.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RemoteTableQuery<DocumentVisitorType> implements SchemaMappingQuery {

    private final TableMappingDefinition fromTable;
    private final List<AbstractColumnMappingDefinition> selectList;
    private final QueryPredicate<DocumentVisitorType> selection;

    /**
     * Creates an instance of {@link RemoteTableQuery}.
     * 
     * @param fromTable  remote table to query
     * @param selectList in correct order
     * @param selection  where clause
     */
    public RemoteTableQuery(final TableMappingDefinition fromTable,
            final List<AbstractColumnMappingDefinition> selectList,
            final QueryPredicate<DocumentVisitorType> selection) {
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
    public QueryPredicate<DocumentVisitorType> getSelection() {
        return this.selection;
    }
}
