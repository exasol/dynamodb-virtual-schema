package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMappingDefinition}s from exasol document mapping language definitions. If the
 * definition contains nested lists that are mapped using a {@code ToTableMapping} the nested table is built using a
 * recursive call to {@link NestedTableMappingFactory}.
 */
public abstract class AbstractTableMappingFactory {
    protected static final String MAPPING_KEY = "mapping";
    protected static final String DEST_TABLE_NAME_KEY = "destTable";
    private static final String FIELDS_KEY = "fields";
    private static final String TO_STRING_MAPPING_KEY = "toStringMapping";
    private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";
    private static final String TO_TABLE_MAPPING_KEY = "toTableMapping";
    /**
     * Building tables for nested lists is delayed using this queue as they need the completely built table that maps
     * the object they are nested in.
     */
    private final List<NestedTableReader> nestedTableReaderQueue = new ArrayList<>();

    /**
     * Reads a table definition from an exasol document mapping language definition. If nested lists are mapped using a
     * {@code ToTableMapping}, multiple tables are returned.
     *
     * @param definition exasol document mapping language definition
     * @throws ExasolDocumentMappingLanguageException if schema mapping definition is invalid
     */
    public final List<TableMappingDefinition> readMappingDefinition(final JsonObject definition) {
        final List<TableMappingDefinition> tables = new ArrayList<>();
        final TableMappingDefinition rootTable = readTable(definition);
        tables.add(rootTable);
        for (final NestedTableReader nestedTableReader : this.nestedTableReaderQueue) {
            tables.addAll(nestedTableReader.readNestedTable(rootTable));
        }
        return tables;
    }

    protected abstract TableMappingDefinition readTable(final JsonObject definition);

    protected final void visitMapping(final JsonObject definition, final DocumentPathExpression.Builder sourcePath,
            final TableMappingDefinition.Builder tableBuilder, final String propertyName, final boolean isRootLevel) {
        final JsonColumnMappingFactory columnMappingFactory = new JsonColumnMappingFactory();
        switch (getMappingType(definition)) {
        case TO_STRING_MAPPING_KEY:
            columnMappingFactory.addStringColumnIfPossible(definition.getJsonObject(TO_STRING_MAPPING_KEY), sourcePath,
                    tableBuilder, propertyName, isRootLevel);
            break;
        case TO_JSON_MAPPING_KEY:
            columnMappingFactory.addToJsonColumn(definition.getJsonObject(TO_JSON_MAPPING_KEY), sourcePath,
                    tableBuilder, propertyName);
            break;
        case TO_TABLE_MAPPING_KEY:
            queueAddingNestedTable(definition.getJsonObject(TO_TABLE_MAPPING_KEY), sourcePath, propertyName);
            break;
        case FIELDS_KEY:
            visitChildren(definition.getJsonObject(FIELDS_KEY), sourcePath, tableBuilder);
            break;
        case "":// no mapping definition
            break;
        default:
            throw new UnsupportedOperationException("This mapping type is not supported in the current version.");
        }
    }

    private void queueAddingNestedTable(final JsonObject definition, final DocumentPathExpression.Builder sourcePath,
            final String propertyName) {
        this.nestedTableReaderQueue
                .add(parentTable -> new NestedTableMappingFactory(parentTable, propertyName, sourcePath)
                        .readMappingDefinition(definition));
    }

    private String getMappingType(final JsonObject definition) {
        final Set<String> keys = definition.keySet();
        if (keys.isEmpty()) {
            return "";
        } else if (keys.size() == 1) {
            return keys.iterator().next();
        } else {
            throw new ExasolDocumentMappingLanguageException("Please, define only one mapping for one property.");
        }
    }

    private void visitChildren(final JsonObject definition, final DocumentPathExpression.Builder sourcePath,
            final TableMappingDefinition.Builder tableBuilder) {
        for (final String dynamodbPropertyName : definition.keySet()) {
            final DocumentPathExpression.Builder newBuilder = new DocumentPathExpression.Builder(sourcePath)
                    .addObjectLookup(dynamodbPropertyName);
            visitMapping(definition.getJsonObject(dynamodbPropertyName), newBuilder, tableBuilder, dynamodbPropertyName,
                    false);
        }
    }

    protected static interface NestedTableReader {
        List<TableMappingDefinition> readNestedTable(TableMappingDefinition parentTable);
    }
}
