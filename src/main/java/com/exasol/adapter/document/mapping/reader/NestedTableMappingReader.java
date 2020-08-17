package com.exasol.adapter.document.mapping.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.JsonObject;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.IterationIndexColumnMapping;
import com.exasol.adapter.document.mapping.TableMapping;

/**
 * This class builds {@link TableMapping}s from Exasol document mapping language definitions for nested lists. It is
 * called from {@link AbstractTableMappingReader} when a {@code ToTableMapping} is found.
 */
class NestedTableMappingReader extends AbstractTableMappingReader {
    final DocumentPathExpression.Builder tablesSourcePath;
    private final GlobalKey parentsKey;
    private final TableMapping parentTable;
    private final String tableName;

    /**
     * Create an instance of {@link NestedTableMappingReader}.
     *
     * @param definition                  exasol document mapping language definition
     * @param parentTable                 the parent table that maps the object in that the nested list (that this table
     *                                    maps) is nested in
     * @param containingListsPropertyName the property name of the nested list that this table maps
     * @param sourcePath                  the path to the nested list that this table maps
     * @param parentsKey                  parent's key columns. This must always be a global key. If the parent has a
     *                                    local key, the foreign key must be added to make it global
     */
    NestedTableMappingReader(final JsonObject definition, final TableMapping parentTable,
            final String containingListsPropertyName, final DocumentPathExpression.Builder sourcePath,
            final GlobalKey parentsKey) {
        this.parentTable = parentTable;
        this.tablesSourcePath = new DocumentPathExpression.Builder(sourcePath).addArrayAll();
        this.parentsKey = parentsKey;
        this.tableName = getNestedTableName(definition, this.parentTable.getExasolName(), containingListsPropertyName);
        readMappingDefinition(definition);
    }

    @Override
    protected TableMapping createTable(final List<ColumnMapping> columns) {
        final List<ColumnMapping> columnsWithForeignKey = addForeignKeyColumnsToColumnListIfNotPresent(columns);
        return new TableMapping(this.tableName, this.parentTable.getRemoteName(), columnsWithForeignKey,
                getPathToTable().build());
    }

    private List<ColumnMapping> addForeignKeyColumnsToColumnListIfNotPresent(final List<ColumnMapping> columns) {
        final List<ColumnMapping> columnsWithForeignKey = new ArrayList<>(columns);
        for (final ColumnMapping foreignKeyColumn : getForeignKey()) {
            if (!columnsWithForeignKey.contains(foreignKeyColumn)) {
                columnsWithForeignKey.add(foreignKeyColumn);
            }
        }
        return columnsWithForeignKey;
    }

    @Override
    protected GlobalKey generateGlobalKey(final List<ColumnMapping> availableColumns) {
        final IterationIndexColumnMapping indexColumn = new IterationIndexColumnMapping("INDEX",
                getPathToTable().build());
        return new GlobalKey(getForeignKey(), List.of(indexColumn));
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
    protected List<ColumnMapping> getForeignKey() {
        return Stream
                .concat(this.parentsKey.getForeignKeyColumns().stream(),
                        this.parentsKey.getOwnKeyColumns().stream()
                                .map(column -> column.withNewExasolName(
                                        this.parentTable.getExasolName() + "_" + column.getExasolColumnName())))
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
