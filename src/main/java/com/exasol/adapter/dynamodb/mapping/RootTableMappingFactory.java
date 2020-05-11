package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMappingDefinition}s from Exasol document mapping language definitions. In contrast to
 * {@link NestedTableMappingFactory} this class handles the whole definition object.
 */
public class RootTableMappingFactory extends AbstractTableMappingFactory {
    private static final String SRC_TABLE_NAME_KEY = "srcTable";

    @Override
    protected TableMappingDefinition createTable(final JsonObject definition,
            final List<ColumnMappingDefinition> columns) {
        return new TableMappingDefinition(definition.getString(DEST_TABLE_NAME_KEY),
                definition.getString(SRC_TABLE_NAME_KEY), columns, getPathToTable().build());
    }

    @Override
    protected DocumentPathExpression.Builder getPathToTable() {
        return new DocumentPathExpression.Builder();
    }

    @Override
    protected boolean isNestedTable() {
        return false;
    }

}
