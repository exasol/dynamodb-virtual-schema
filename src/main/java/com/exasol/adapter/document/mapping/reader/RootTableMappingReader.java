package com.exasol.adapter.document.mapping.reader;

import java.util.Collections;
import java.util.List;

import javax.json.JsonObject;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.TableKeyFetcher;
import com.exasol.adapter.document.mapping.TableMapping;

/**
 * This class builds {@link TableMapping}s from Exasol document mapping language definitions. In contrast to
 * {@link NestedTableMappingReader} this class handles the whole definition object.
 */
class RootTableMappingReader extends AbstractTableMappingReader {
    private static final String SRC_TABLE_NAME_KEY = "source";
    private final TableKeyFetcher tableKeyFetcher;
    private final String exasolTableName;
    private final String remoteTableName;

    /**
     * Create an {@link RootTableMappingReader} that reads a table definition from an Exasol document mapping language
     * definition. If nested lists are mapped using a {@code ToTableMapping}, multiple tables are read. The read tables
     * can be retrieved using {@link #getTables()}.
     *
     * @param definition      exasol document mapping language definition
     * @param tableKeyFetcher remote database specific {@link TableKeyFetcher}
     * @throws ExasolDocumentMappingLanguageException if schema mapping definition is invalid
     */
    public RootTableMappingReader(final JsonObject definition, final TableKeyFetcher tableKeyFetcher) {
        this.tableKeyFetcher = tableKeyFetcher;
        this.exasolTableName = definition.getString(DEST_TABLE_NAME_KEY);
        this.remoteTableName = definition.getString(SRC_TABLE_NAME_KEY);
        readMappingDefinition(definition);
    }

    @Override
    protected TableMapping createTable(final List<ColumnMapping> columns) {
        return new TableMapping(this.exasolTableName, this.remoteTableName, columns, getPathToTable().build());
    }

    @Override
    protected GlobalKey generateGlobalKey(final List<ColumnMapping> availableColumns) {
        try {
            return new GlobalKey(Collections.emptyList(),
                    this.tableKeyFetcher.fetchKeyForTable(this.remoteTableName, availableColumns));
        } catch (final TableKeyFetcher.NoKeyFoundException e) {
            throw new ExasolDocumentMappingLanguageException("Could not infer keys for table " + this.exasolTableName
                    + ". Please define a unique key by setting key='global' for one or more columns.");
        }
    }

    @Override
    protected DocumentPathExpression.Builder getPathToTable() {
        return DocumentPathExpression.builder();
    }

    @Override
    protected boolean isNestedTable() {
        return false;
    }

    @Override
    protected List<ColumnMapping> getForeignKey() {
        throw new ExasolDocumentMappingLanguageException(
                "Local keys make no sense in root table mapping definitions. Please make this key global.");
    }
}
