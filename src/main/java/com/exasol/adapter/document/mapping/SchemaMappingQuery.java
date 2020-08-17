package com.exasol.adapter.document.mapping;

import java.util.List;

/**
 * This interface defines the access to the schema mapping relevant information of a query.
 */
public interface SchemaMappingQuery {

    /**
     * Get the table defined in the {@code FROM} clause of the statement.
     *
     * @return {{@link TableMapping}}
     */
    public TableMapping getFromTable();

    /**
     * Get all columns that must be fetched from the remote database. These are the columns that are selected and the
     * columns that are compared in the post selection.
     *
     * @return set {@link ColumnMapping}s.
     */
    public List<ColumnMapping> getRequiredColumns();
}
