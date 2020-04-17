package com.exasol.adapter.dynamodb.mapping;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.utils.StringSerializer;

/**
 * This class converts a {@link SchemaMappingDefinition} into a {@link SchemaMetadata}. The
 * {@link AbstractColumnMappingDefinition}s are serialized into {@link ColumnMetadata#getAdapterNotes()}. Using
 * {@link #convertBackColumn(ColumnMetadata)} it can get deserialized again.
 */
public class SchemaMappingDefinitionToSchemaMetadataConverter {

    /**
     * Creates a {@link SchemaMetadata} for a given {@link SchemaMappingDefinition}
     *
     * @param schemaMappingDefinition the {@link SchemaMappingDefinition} to be converted
     * @return {@link SchemaMetadata}
     * @throws IOException if {@link AbstractColumnMappingDefinition} could not get serialized
     */
    public SchemaMetadata convert(final SchemaMappingDefinition schemaMappingDefinition) throws IOException {
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        /* The HashMap is used here instead of the List interface because it is serializable. */
        final HashMap<String, TableMappingDefinition> tableMappings = new HashMap<>();
        for (final TableMappingDefinition table : schemaMappingDefinition.getTableMappings()) {
            tableMetadata.add(convertTable(table));
            tableMappings.put(table.getExasolName(), table);
        }
        @SuppressWarnings("java:S125") //not commented out code
        /*
         * Actually the tables should be serialized into TableSchema adapter notes. But as these do not work due to a
         * bug, they are added here. {@see https://github.com/exasol/dynamodb-virtual-schema/issues/25}
         */
        final String serialized = StringSerializer.serializeToString(new TableMappings(tableMappings));
        return new SchemaMetadata(serialized, tableMetadata);
    }

    private TableMetadata convertTable(final TableMappingDefinition tableMappingDefinition) throws IOException {
        final List<ColumnMetadata> columnDefinitions = new ArrayList<>();
        for (final AbstractColumnMappingDefinition column : tableMappingDefinition.getColumns()) {
            columnDefinitions.add(convertColumn(column));
        }
        final String adapterNotes = "";// Due to a bug in exasol core adapter notes are not stored for tables
        return new TableMetadata(tableMappingDefinition.getExasolName(), adapterNotes, columnDefinitions, "");
    }

    private ColumnMetadata convertColumn(final AbstractColumnMappingDefinition columnMappingDefinition)
            throws IOException {
        final String serialized = StringSerializer.serializeToString(columnMappingDefinition);
        return ColumnMetadata.builder()//
                .name(columnMappingDefinition.getExasolColumnName())//
                .type(columnMappingDefinition.getExasolDataType())//
                .defaultValue(columnMappingDefinition.getExasolDefaultValueLiteral())//
                .nullable(columnMappingDefinition.isExasolColumnNullable())//
                .adapterNotes(serialized).build();
    }

    /**
     * Deserializes a {@link TableMappingDefinition} from {@link TableMetadata}.
     *
     * @param tableMetadata  metadata for the table to be deserialized
     * @param schemaMetadata needed because the tables can't be serialized into the TableMetadata due to a bug
     * @return deserialized {@link TableMappingDefinition}
     * @throws IllegalStateException if deserialization fails
     */
    public TableMappingDefinition convertBackTable(final TableMetadata tableMetadata,
            final SchemaMetadata schemaMetadata) {
        try {
            return convertBackTableIntern(tableMetadata, schemaMetadata);
        } catch (final IOException | ClassNotFoundException exception) {
            throw new IllegalStateException("Failed to deserialize TableMappingDefinition.", exception);
        }
    }

    private TableMappingDefinition convertBackTableIntern(final TableMetadata tableMetadata,
            final SchemaMetadata schemaMetadata) throws IOException, ClassNotFoundException {
        final TableMappingDefinition preliminaryTable = findTableInSchemaMetadata(tableMetadata.getName(), schemaMetadata);
        /*
         * As the columns are transient in TableMappingDefinition, they must be deserialized from the ColumnMetadata and
         * added separately.
         */
        final List<AbstractColumnMappingDefinition> columns = new ArrayList<>(tableMetadata.getColumns().size());
        for (final ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            columns.add(convertBackColumn(columnMetadata));
        }
        return new TableMappingDefinition(preliminaryTable, columns);
    }

    /**
     * Workaround as tables cant be serialized to {@link TableMetadata} due to a bug in Exasol.
     * {@see https://github.com/exasol/dynamodb-virtual-schema/issues/25}
     */
    private TableMappingDefinition findTableInSchemaMetadata(final String tableName,
            final SchemaMetadata schemaMetadata) throws IOException, ClassNotFoundException {
        final String serialized = schemaMetadata.getAdapterNotes();
        final TableMappings tableMappings = (TableMappings) StringSerializer.deserializeFromString(serialized);
        return tableMappings.mappings.get(tableName);
    }

    /**
     * Deserializes a {@link AbstractColumnMappingDefinition} from {@link ColumnMetadata}.
     *
     * @param columnMetadata {@link ColumnMetadata} to deserialized from
     * @return ColumnMappingDefinition
     * @throws IllegalStateException if deserialization fails
     */
    public AbstractColumnMappingDefinition convertBackColumn(final ColumnMetadata columnMetadata) {
        try {
            final String serialized = columnMetadata.getAdapterNotes();
            return (AbstractColumnMappingDefinition) StringSerializer.deserializeFromString(serialized);
        } catch (final IOException | ClassNotFoundException exception) {
            throw new IllegalStateException("Failed to deserialize ColumnMappingDefinition.", exception);
        }
    }

    /**
     * This class is used as a fix for the bug because of which {@link TableMetadata} can't store adapter notes.
     * {@see https://github.com/exasol/dynamodb-virtual-schema/issues/25}. It gets serialized in the
     * {@link SchemaMetadata} and stores a map that gives the {@link TableMappingDefinition} for its Exasol table name.
     */
    private static class TableMappings implements Serializable {
        private static final long serialVersionUID = -6920869661356098960L;
        private final HashMap<String, TableMappingDefinition> mappings;

        private TableMappings(final HashMap<String, TableMappingDefinition> mappings) {
            this.mappings = mappings;
        }
    }
}
