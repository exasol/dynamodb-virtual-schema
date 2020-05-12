package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMapping}s from Exasol document mapping language definitions for nested lists. It is
 * called from {@link AbstractTableMappingReader} when a {@code ToTableMapping} is found.
 */
class NestedTableMappingReader extends AbstractTableMappingReader {
    final DocumentPathExpression.Builder tablesSourcePath;
    private final List<ColumnMapping> parentKeyColumns;
    private final TableMapping parentTable;
    private final String tableName;

    /**
     * Creates an instance of {@link NestedTableMappingReader}.
     *
     * @param definition                  exasol document mapping language definition
     * @param parentTable                 the parent table that maps the object in that the nested list (that this table
     *                                    maps) is nested in
     * @param containingListsPropertyName the property name of the nested list that this table maps
     * @param sourcePath                  the path to the nested list that this table maps
     * @param parentKeyColumns            list of parent's key columns. This must always be a global key. If the parent
     *                                    has a local key, the foreign key must be added to make it global
     */
    NestedTableMappingReader(final JsonObject definition, final TableMapping parentTable,
            final String containingListsPropertyName, final DocumentPathExpression.Builder sourcePath,
            final List<ColumnMapping> parentKeyColumns) {
        this.parentTable = parentTable;
        this.tablesSourcePath = new DocumentPathExpression.Builder(sourcePath).addArrayAll();
        this.parentKeyColumns = parentKeyColumns;
        this.tableName = getNestedTableName(definition, this.parentTable.getExasolName(), containingListsPropertyName);
        readMappingDefinition(definition);
    }

    @Override
    protected TableMapping createTable(final List<ColumnMapping> columns) {
        // TODO add foreign key columns
        return new TableMapping(this.tableName, this.parentTable.getRemoteName(), columns, getPathToTable().build());
    }

    @Override
    protected List<ColumnMapping> generateGlobalKeyColumns() {
        final List<ColumnMapping> keys = new ArrayList<>(getForeignKey());
        final String indexColumnName = this.tableName + "_" + "INDEX";
        keys.add(new IterationIndexColumnMapping(indexColumnName, getPathToTable().build()));
        return keys;
    }

    @Override
    protected DocumentPathExpression.Builder getPathToTable() {
        return this.tablesSourcePath;
    }

    @Override
    protected boolean isNestedTable() {
        return true;
    }

    @Override
    protected List<ColumnMapping> makeLocalKeyGlobal(final List<ColumnMapping> localKeyColumns) {
        final List<ColumnMapping> globalKeyColumns = new ArrayList<>();
        globalKeyColumns.addAll(getForeignKey());
        globalKeyColumns.addAll(localKeyColumns);
        return globalKeyColumns;
    }

    private List<ColumnMapping> getForeignKey() {
        return this.parentKeyColumns.stream()
                .map(column -> column
                        .copyWithNewExasolName(this.parentTable.getExasolName() + "_" + column.getExasolColumnName()))
                .collect(Collectors.toList());
    }

    private String getNestedTableName(final JsonObject definition, final String parentTableExasolName,
            final String propertyName) {
        return definition.getString(DEST_TABLE_NAME_KEY,
                getNestedTableNameDefaultName(parentTableExasolName, propertyName));
    }

    private String getNestedTableNameDefaultName(final String parentTableExasolName, final String propertyName) {
        return parentTableExasolName + "_" + propertyName.toUpperCase();
    }
}
