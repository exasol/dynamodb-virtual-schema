package com.exasol.adapter.dynamodb.mapping;

import java.util.*;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMapping}s from Exasol document mapping language definitions. If the definition contains
 * nested lists that are mapped using a {@code ToTableMapping}, then the nested table is built using a recursive call to
 * {@link NestedTableMappingReader}.
 */
public abstract class AbstractTableMappingReader {
    protected static final String DEST_TABLE_NAME_KEY = "destTable";
    private static final String MAPPING_KEY = "mapping";
    final List<TableMapping> tables = new ArrayList<>();

    protected void readMappingDefinition(final JsonObject definition) {
        final SchemaMappingDefinitionLanguageVisitor visitor = new SchemaMappingDefinitionLanguageVisitor();
        visitor.visitMapping(definition.getJsonObject(MAPPING_KEY), getPathToTable(), null, !isNestedTable());
        if (visitor.hasNestedTables()) {
            readTableWithNestedTable(visitor);
        } else {
            this.tables.add(createTable(visitor.getAllColumns()));
        }
    }

    private void readTableWithNestedTable(final SchemaMappingDefinitionLanguageVisitor visitor) {
        final GlobalKey globalKey = buildGlobalKey(visitor);
        final List<ColumnMapping> allColumns = new ArrayList<>(globalKey.getOwnKeyColumns());
        allColumns.addAll(globalKey.getForeignKeyColumns());
        allColumns.addAll(visitor.getNonKeyColumns());
        final TableMapping rootTable = createTable(allColumns);
        this.tables.add(rootTable);
        for (final NestedTableReader nestedTableReader : visitor.getNestedTableReaderQueue()) {
            this.tables.addAll(nestedTableReader.readNestedTable(rootTable, globalKey));
        }
    }

    private GlobalKey buildGlobalKey(final SchemaMappingDefinitionLanguageVisitor visitor) {
        final List<ColumnMapping> userDefinedKeyColumns = visitor.getKeyColumns();
        if (userDefinedKeyColumns.isEmpty()) {
            return generateGlobalKey(visitor.getAllColumns());
        } else {
            if (visitor.getKeyType().equals(ColumnMappingDefinitionKeyTypeReader.KeyType.LOCAL)) {
                return new GlobalKey(getForeignKey(), userDefinedKeyColumns);
            } else {
                return new GlobalKey(Collections.emptyList(), userDefinedKeyColumns);
            }
        }
    }

    public List<TableMapping> getTables() {
        return this.tables;
    }

    protected abstract TableMapping createTable(List<ColumnMapping> columns);

    /**
     * This method is called if no key columns were defined in the schema mapping definition but key columns are
     * required (for a nested table).
     *
     * @param availableColumns list of column mappings that could be used for building a key
     * @return list containing the generated key columns
     */
    protected abstract GlobalKey generateGlobalKey(List<ColumnMapping> availableColumns);

    protected abstract DocumentPathExpression.Builder getPathToTable();

    protected abstract boolean isNestedTable();

    protected abstract List<ColumnMapping> getForeignKey();

    @FunctionalInterface
    private static interface NestedTableReader {
        List<TableMapping> readNestedTable(TableMapping parentTable, GlobalKey parentKeyColumns);
    }

    protected static class GlobalKey {
        private final List<ColumnMapping> foreignKeyColumns;
        private final List<ColumnMapping> ownKeyColumns;

        public GlobalKey(final List<ColumnMapping> foreignKeyColumns, final List<ColumnMapping> ownKeyColumns) {
            this.foreignKeyColumns = foreignKeyColumns;
            this.ownKeyColumns = ownKeyColumns;
        }

        public List<ColumnMapping> getForeignKeyColumns() {
            return this.foreignKeyColumns;
        }

        public List<ColumnMapping> getOwnKeyColumns() {
            return this.ownKeyColumns;
        }
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
        private final Queue<NestedTableReader> nestedTableReaderQueue;
        private final List<ColumnMapping> nonKeyColumns;
        private final List<ColumnMapping> keyColumns;
        private ColumnMappingDefinitionKeyTypeReader.KeyType keyType;

        public SchemaMappingDefinitionLanguageVisitor() {
            this.nestedTableReaderQueue = new LinkedList<>();
            this.nonKeyColumns = new ArrayList<>();
            this.keyColumns = new ArrayList<>();
            this.keyType = ColumnMappingDefinitionKeyTypeReader.KeyType.NO_KEY;
        }

        public final void visitMapping(final JsonObject definition, final DocumentPathExpression.Builder sourcePath,
                final String propertyName, final boolean isRootLevel) {
            final JsonColumnMappingReader columnMappingFactory = new JsonColumnMappingReader();
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

        private void addColumn(final ColumnMapping column, final JsonObject definition,
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
            this.nestedTableReaderQueue.add((parentTable, parentKeyColumns) -> new NestedTableMappingReader(definition,
                    parentTable, propertyName, sourcePath, parentKeyColumns).getTables());
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

        public List<ColumnMapping> getAllColumns() {
            final List<ColumnMapping> union = new ArrayList<>();
            union.addAll(this.keyColumns);
            union.addAll(this.nonKeyColumns);
            return union;
        }

        public List<ColumnMapping> getKeyColumns() {
            return this.keyColumns;
        }

        public List<ColumnMapping> getNonKeyColumns() {
            return this.nonKeyColumns;
        }

        public ColumnMappingDefinitionKeyTypeReader.KeyType getKeyType() {
            return this.keyType;
        }

        public Queue<NestedTableReader> getNestedTableReaderQueue() {
            return this.nestedTableReaderQueue;
        }

        public boolean hasNestedTables() {
            return !this.nestedTableReaderQueue.isEmpty();
        }
    }
}
