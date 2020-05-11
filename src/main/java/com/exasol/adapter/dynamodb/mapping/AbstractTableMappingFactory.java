package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMappingDefinition}s from Exasol document mapping language definitions. If the
 * definition contains nested lists that are mapped using a {@code ToTableMapping}, then the nested table is built using
 * a recursive call to {@link NestedTableMappingFactory}.
 */
public abstract class AbstractTableMappingFactory {
    protected static final String DEST_TABLE_NAME_KEY = "destTable";
    private static final String MAPPING_KEY = "mapping";

    /**
     * Reads a table definition from an exasol document mapping language definition. If nested lists are mapped using a
     * {@code ToTableMapping}, multiple tables are returned.
     *
     * @param definition exasol document mapping language definition
     * @throws ExasolDocumentMappingLanguageException if schema mapping definition is invalid
     */
    public final List<TableMappingDefinition> readMappingDefinition(final JsonObject definition) {
        final List<TableMappingDefinition> tables = new ArrayList<>();
        final SchemaMappingDefinitionLanguageVisitor visitor = new SchemaMappingDefinitionLanguageVisitor();
        visitor.visitMapping(definition.getJsonObject(MAPPING_KEY), getPathToTable(), null, !isNestedTable());

        final TableMappingDefinition rootTable = createTable(definition, visitor.getAllColumns());
        tables.add(rootTable);
        for (final NestedTableReader nestedTableReader : visitor.getNestedTableReaderQueue()) {
            tables.addAll(nestedTableReader.readNestedTable(rootTable));
        }
        return tables;
    }

    protected abstract TableMappingDefinition createTable(final JsonObject definition,
            List<ColumnMappingDefinition> columns);

    protected abstract DocumentPathExpression.Builder getPathToTable();

    protected abstract boolean isNestedTable();

    @FunctionalInterface
    private static interface NestedTableReader {
        List<TableMappingDefinition> readNestedTable(TableMappingDefinition parentTable);
    }

    private static class SchemaMappingDefinitionLanguageVisitor {
        private static final String FIELDS_KEY = "fields";
        private static final String TO_STRING_MAPPING_KEY = "toStringMapping";
        private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";
        private static final String TO_TABLE_MAPPING_KEY = "toTableMapping";

        /**
         * Building tables for nested lists is delayed using this queue as they need the completely built table that
         * maps the object they are nested in.
         */
        private final List<NestedTableReader> nestedTableReaderQueue;
        private final List<ColumnMappingDefinition> nonKeyColumns;
        private final List<ColumnMappingDefinition> keyColumns;
        private boolean hasNestedTable;
        private ColumnMappingDefinitionKeyTypeReader.KeyType keyType;

        public SchemaMappingDefinitionLanguageVisitor() {
            this.nestedTableReaderQueue = new ArrayList<>();
            this.hasNestedTable = false;
            this.nonKeyColumns = new ArrayList<>();
            this.keyColumns = new ArrayList<>();
            this.keyType = ColumnMappingDefinitionKeyTypeReader.KeyType.NO_KEY;
        }

        public final void visitMapping(final JsonObject definition, final DocumentPathExpression.Builder sourcePath,
                final String propertyName, final boolean isRootLevel) {
            final JsonColumnMappingFactory columnMappingFactory = new JsonColumnMappingFactory();
            switch (getMappingType(definition, sourcePath)) {
            case TO_STRING_MAPPING_KEY:
                final JsonObject toStringDefinition = definition.getJsonObject(TO_STRING_MAPPING_KEY);
                addColumn(columnMappingFactory.readStringColumnIfPossible(toStringDefinition, sourcePath, propertyName,
                        isRootLevel), toStringDefinition, sourcePath);
                break;
            case TO_JSON_MAPPING_KEY:
                final JsonObject toJsonDefinition = definition.getJsonObject(TO_JSON_MAPPING_KEY);
                addColumn(columnMappingFactory.readToJsonColumn(toJsonDefinition, sourcePath, propertyName),
                        toJsonDefinition, sourcePath);
                break;
            case TO_TABLE_MAPPING_KEY:
                queueAddingNestedTable(definition.getJsonObject(TO_TABLE_MAPPING_KEY), sourcePath, propertyName);
                break;
            case FIELDS_KEY:
                visitChildren(definition.getJsonObject(FIELDS_KEY), sourcePath);
                break;
            case "":// no mapping definition
                break;
            default:
                throw new UnsupportedOperationException("This mapping type is not supported in the current version.");
            }
        }

        private void addColumn(final ColumnMappingDefinition column, final JsonObject definition,
                final DocumentPathExpression.Builder sourcePath) {
            final ColumnMappingDefinitionKeyTypeReader.KeyType columnsKeyType = new ColumnMappingDefinitionKeyTypeReader()
                    .readKeyType(definition);
            if (columnsKeyType.equals(ColumnMappingDefinitionKeyTypeReader.KeyType.NO_KEY)) {
                this.nonKeyColumns.add(column);
            } else {
                if (this.keyType != columnsKeyType
                        && this.keyType != ColumnMappingDefinitionKeyTypeReader.KeyType.NO_KEY) {
                    throw new ExasolDocumentMappingLanguageException(sourcePath.build().toString()
                            + ": This table already has a key of different type (global/local). "
                            + "Please either define all keys of the table local or global.");
                }
                this.keyType = columnsKeyType;
                this.keyColumns.add(column);
            }
        }

        private void queueAddingNestedTable(final JsonObject definition,
                final DocumentPathExpression.Builder sourcePath, final String propertyName) {
            this.hasNestedTable = true;
            this.nestedTableReaderQueue
                    .add(parentTable -> new NestedTableMappingFactory(parentTable, propertyName, sourcePath)
                            .readMappingDefinition(definition));
        }

        private String getMappingType(final JsonObject definition, final DocumentPathExpression.Builder sourcePath) {
            final Set<String> keys = definition.keySet();
            if (keys.isEmpty()) {
                return "";
            } else if (keys.size() == 1) {
                return keys.iterator().next();
            } else {
                throw new ExasolDocumentMappingLanguageException(
                        sourcePath.build().toString() + ": Please define only one mapping for one property.");
            }
        }

        private void visitChildren(final JsonObject definition, final DocumentPathExpression.Builder sourcePath) {
            for (final String dynamodbPropertyName : definition.keySet()) {
                final DocumentPathExpression.Builder newBuilder = new DocumentPathExpression.Builder(sourcePath)
                        .addObjectLookup(dynamodbPropertyName);
                visitMapping(definition.getJsonObject(dynamodbPropertyName), newBuilder, dynamodbPropertyName, false);
            }
        }

        public List<ColumnMappingDefinition> getAllColumns() {
            final List<ColumnMappingDefinition> union = new ArrayList<>();
            union.addAll(this.keyColumns);
            union.addAll(this.nonKeyColumns);
            return union;
        }

        public List<NestedTableReader> getNestedTableReaderQueue() {
            return this.nestedTableReaderQueue;
        }

        public boolean isHasNestedTable() {
            return this.hasNestedTable;
        }
    }
}
