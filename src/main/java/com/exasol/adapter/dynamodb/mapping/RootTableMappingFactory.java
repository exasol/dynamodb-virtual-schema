package com.exasol.adapter.dynamodb.mapping;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMappingDefinition}s from Exasol document mapping language definitions. In contrast to
 * {@link NestedTableMappingFactory} this class handles the whole definition object.
 */
public class RootTableMappingFactory extends AbstractTableMappingFactory {
    private static final String SRC_TABLE_NAME_KEY = "srcTable";

    @Override
    protected TableMappingDefinition readTable(final JsonObject definition) {
        final TableMappingDefinition.Builder tableBuilder = TableMappingDefinition
                .rootTableBuilder(definition.getString(DEST_TABLE_NAME_KEY), definition.getString(SRC_TABLE_NAME_KEY));
        visitMapping(definition.getJsonObject(MAPPING_KEY), new DocumentPathExpression.Builder(), tableBuilder, null,
                true);
        return tableBuilder.build();
    }
}
