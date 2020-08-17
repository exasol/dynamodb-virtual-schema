package com.exasol.adapter.document.queryplanning;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.SchemaMappingQuery;
import com.exasol.adapter.document.mapping.TableMapping;
import com.exasol.adapter.document.querypredicate.InvolvedColumnCollector;
import com.exasol.adapter.document.querypredicate.QueryPredicate;

/**
 * This class represents the whole query inside of one document.
 */
public class RemoteTableQuery implements SchemaMappingQuery, Serializable {
    private static final long serialVersionUID = 3383108914924893151L;
    private final TableMapping fromTable;
    private final List<ColumnMapping> selectList;
    private final List<ColumnMapping> requiredColumns;
    private final transient QueryPredicate pushDownSelection; // TODO refactor; transient and non transient part
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
        this.requiredColumns = calculateRequiredColumns();
    }

    @Override
    public TableMapping getFromTable() {
        return this.fromTable;
    }

    public List<ColumnMapping> calculateRequiredColumns() {
        final List<ColumnMapping> postSelectionsColumns = new InvolvedColumnCollector()
                .collectInvolvedColumns(getPostSelection());
        return Stream.concat(postSelectionsColumns.stream(), getSelectList().stream()).distinct()
                .sorted(Comparator.comparing(ColumnMapping::getExasolColumnName)).collect(Collectors.toList());
    }

    @Override
    public List<ColumnMapping> getRequiredColumns() {
        return this.requiredColumns;
    }

    /**
     * Get the select list columns.
     *
     * @return select list columns
     */
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
