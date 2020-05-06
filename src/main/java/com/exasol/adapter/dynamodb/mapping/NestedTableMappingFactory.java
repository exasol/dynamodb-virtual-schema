package com.exasol.adapter.dynamodb.mapping;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMappingDefinition}s from exasol document mapping language definitions for nested lists.
 * It is called from {@link AbstractTableMappingFactory} when a {@code ToTableMapping} was found.
 */
class NestedTableMappingFactory extends AbstractTableMappingFactory {
    private final TableMappingDefinition parentTable;
    private final String containingListsPropertyName;
    private final DocumentPathExpression.Builder sourcePath;

    /**
     * Creates an instance of {@link NestedTableMappingFactory}.
     * 
     * @param parentTable                 the table mapping the object that list mapped by this table is nested in
     * @param containingListsPropertyName the property name of the nested list that this table maps
     * @param sourcePath                  the path to the nested list that this table maps
     */
    public NestedTableMappingFactory(final TableMappingDefinition parentTable, final String containingListsPropertyName,
            final DocumentPathExpression.Builder sourcePath) {
        this.parentTable = parentTable;
        this.containingListsPropertyName = containingListsPropertyName;
        this.sourcePath = sourcePath;
    }

    @Override
    protected TableMappingDefinition readTable(final JsonObject definition) {
        final DocumentPathExpression.Builder tablesSourcePath = new DocumentPathExpression.Builder(this.sourcePath)
                .addArrayAll();
        final String tableName = getNestedTableName(definition, this.parentTable.getExasolName(),
                this.containingListsPropertyName);
        final TableMappingDefinition.Builder nestedTableBuilder = TableMappingDefinition.nestedTableBuilder(tableName,
                this.parentTable.getRemoteName(), tablesSourcePath.build());
        visitMapping(definition.getJsonObject(MAPPING_KEY), tablesSourcePath, nestedTableBuilder, null, false);
        return nestedTableBuilder.build();
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
