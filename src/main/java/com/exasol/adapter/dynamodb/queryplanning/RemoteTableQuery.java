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
public class RemoteTableQuery implements SchemaMappingQuery, Serializable {
    private static final long serialVersionUID = -721150997859918517L;
    private final TableMapping fromTable;
    private final List<ColumnMapping> selectList;
    private final transient QueryPredicate pushDownSelection; // TODO refactor
    private final transient QueryPredicate postSelection;

    /**
     * Create an instance of {@link RemoteTableQuery}.
     * 
     * @param fromTable         remote table to query
     * @param selectList        in correct order
     * @param pushDownSelection the selection that will get pushed down to the remote database
     * @param postSelection     the selection that will be executed by the Exasol database
     */
    public RemoteTableQuery(final TableMapping fromTable, final List<ColumnMapping> selectList,
            final QueryPredicate pushDownSelection, final QueryPredicate postSelection) {
        this.fromTable = fromTable;
        this.selectList = selectList;
        this.pushDownSelection = pushDownSelection;
        this.postSelection = postSelection;
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
    public QueryPredicate getPushDownSelection() {
        return this.pushDownSelection;
    }

    /**
     * Get the selection that is Executed by the Exasol database.
     * 
     * @return Predicate representing the selection
     */
    public QueryPredicate getPostSelection() {
        return this.postSelection;
    }
}
