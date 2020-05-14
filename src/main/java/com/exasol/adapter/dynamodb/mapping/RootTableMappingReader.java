package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link TableMapping}s from Exasol document mapping language definitions. In contrast to
 * {@link NestedTableMappingReader} this class handles the whole definition object.
 */
public class RootTableMappingReader extends AbstractTableMappingReader {
    private static final String SRC_TABLE_NAME_KEY = "srcTable";
    private final JsonObject definition;

    /**
     * Creates an {@link RootTableMappingReader} that reads a table definition from an Exasol document mapping language
     * definition. If nested lists are mapped using a {@code ToTableMapping}, multiple tables are read. The read tables
     * can be retrieved using {@link #getTables()}.
     *
     * @param definition exasol document mapping language definition
     * @throws ExasolDocumentMappingLanguageException if schema mapping definition is invalid
     */
    public RootTableMappingReader(final JsonObject definition) {
        this.definition = definition;
        readMappingDefinition(definition);
    }

    @Override
    protected TableMapping createTable(final List<ColumnMapping> columns) {
        return new TableMapping(this.definition.getString(DEST_TABLE_NAME_KEY),
                this.definition.getString(SRC_TABLE_NAME_KEY), columns, getPathToTable().build());
    }

    @Override
    protected List<ColumnMapping> generateGlobalKeyColumns() {
        throw new UnsupportedOperationException("fetching keys from remote database is not yet supported"); // TODO
    }

    @Override
    protected DocumentPathExpression.Builder getPathToTable() {
        return new DocumentPathExpression.Builder();
    }

    @Override
    protected boolean isNestedTable() {
        return false;
    }

    @Override
    protected List<ColumnMapping> makeLocalKeyGlobal(final List<ColumnMapping> keyColumns) {
        throw new ExasolDocumentMappingLanguageException(
                "Local keys make no sense in root table mapping definitions. Please make this key global.");
    }
}
