package com.exasol.adapter.dynamodb.queryplanning;

import java.io.Serializable;
import java.util.List;

import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingQuery;
import com.exasol.adapter.dynamodb.mapping.TableMapping;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;

/**
 * This class represents the whole query inside of one document.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RemoteTableQuery<DocumentVisitorType> implements SchemaMappingQuery, Serializable {

    private static final long serialVersionUID = 6851292744437631355L;
    private final TableMapping fromTable;
    private final List<ColumnMapping> selectList;
    private final QueryPredicate<DocumentVisitorType> selection;

    /**
     * Create an instance of {@link RemoteTableQuery}.
     * 
     * @param fromTable  remote table to query
     * @param selectList in correct order
     * @param selection  where clause
     */
    public RemoteTableQuery(final TableMapping fromTable, final List<ColumnMapping> selectList,
            final QueryPredicate<DocumentVisitorType> selection) {
        this.fromTable = fromTable;
        this.selectList = selectList;
        this.selection = selection;
    }

    @Override
    public TableMapping getFromTable() {
        return this.fromTable;
    }

    /**
     * Get the select list columns.
     * 
     * @return select list columns
     */
    @Override
    public List<ColumnMapping> getSelectList() {
        return this.selectList;
    }

    /**
     * Get the where clause of this query.
     * 
     * @return Predicate representing the selection
     */
    public QueryPredicate<DocumentVisitorType> getSelection() {
        return this.selection;
    }
}
