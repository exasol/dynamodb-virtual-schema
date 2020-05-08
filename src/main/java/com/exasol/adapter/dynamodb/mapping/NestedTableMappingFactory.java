package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMappingDefinition}s from Exasol document mapping language definitions for nested lists.
 * It is called from {@link AbstractTableMappingFactory} when a {@code ToTableMapping} is found.
 */
class NestedTableMappingFactory extends AbstractTableMappingFactory {
    final DocumentPathExpression.Builder tablesSourcePath;
    private final TableMappingDefinition parentTable;
    private final String containingListsPropertyName;

    /**
     * Creates an instance of {@link NestedTableMappingFactory}.
     * 
     * @param parentTable                 the parent table that maps the object in that the nested list (that this table
     *                                    maps) is nested in
     * @param containingListsPropertyName the property name of the nested list that this table maps
     * @param sourcePath                  the path to the nested list that this table maps
     */
    public NestedTableMappingFactory(final TableMappingDefinition parentTable, final String containingListsPropertyName,
            final DocumentPathExpression.Builder sourcePath) {
        this.parentTable = parentTable;
        this.containingListsPropertyName = containingListsPropertyName;
        this.tablesSourcePath = new DocumentPathExpression.Builder(sourcePath).addArrayAll();
    }

    @Override
    protected TableMappingDefinition createTable(final JsonObject definition,
            final List<ColumnMappingDefinition> columns) {
        final String tableName = getNestedTableName(definition, this.parentTable.getExasolName(),
                this.containingListsPropertyName);
        return new TableMappingDefinition(tableName, this.parentTable.getRemoteName(), columns,
                getPathToTable().build());
    }

    @Override
    protected DocumentPathExpression.Builder getPathToTable() {
        return this.tablesSourcePath;
    }

    @Override
    protected boolean isNestedTable() {
        return true;
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
