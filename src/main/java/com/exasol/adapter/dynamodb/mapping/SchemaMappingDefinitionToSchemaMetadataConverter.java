package com.exasol.adapter.dynamodb.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.StringSerializer;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * This class converts a {@link SchemaMappingDefinition} into a
 * {@link SchemaMetadata}. The {@link ColumnMappingDefinition}s are serialized
 * into {@link ColumnMetadata#getAdapterNotes()}. Using
 * {@link #convertBackColumn(ColumnMetadata)} it can get deserialized again.
 */
public class SchemaMappingDefinitionToSchemaMetadataConverter {
    /**
     * Creates a {@link SchemaMetadata} for a given {@link SchemaMappingDefinition}
     * @param schemaMappingDefinition the {@link SchemaMappingDefinition} to be converted
     * @return {@link SchemaMetadata}
     * @throws IOException if {@link ColumnMappingDefinition} could not get serialized
     */
	public static SchemaMetadata convert(final SchemaMappingDefinition schemaMappingDefinition) throws IOException {
		final List<TableMetadata> tableMetadata = new ArrayList<>();
		for (final TableMappingDefinition table : schemaMappingDefinition.getTableMappings()) {
			tableMetadata.add(convertTable(table));
		}
		return new SchemaMetadata("", tableMetadata);
	}

	private static TableMetadata convertTable(final TableMappingDefinition tableMappingDefinition) throws IOException {
		final List<ColumnMetadata> columnDefinitions = new ArrayList<>();
		for (final ColumnMappingDefinition column : tableMappingDefinition.getColumns()) {
			columnDefinitions.add(convertColumn(column));
		}
		final String adapterNotes = "";// Due to a bug in exasol core adapter notes are not stored for tables
		return new TableMetadata(tableMappingDefinition.getDestName(), adapterNotes, columnDefinitions, "");
	}

	private static ColumnMetadata convertColumn(final ColumnMappingDefinition columnMappingDefinition)
			throws IOException {
		final String serialized = StringSerializer.serializeToString(columnMappingDefinition);
		return ColumnMetadata.builder()//
				.name(columnMappingDefinition.getDestinationName())//
				.type(columnMappingDefinition.getDestinationDataType())//
				.defaultValue(columnMappingDefinition.getDestinationDefaultValue())//
				.nullable(columnMappingDefinition.isDestinationNullable())//
				.adapterNotes(serialized).build();
	}

	/**
	 * Build a {@link ColumnMappingDefinition} from ColumnMetadata.
	 *
	 * @param columnMetadata
	 * @return ColumnMappingDefinition
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static ColumnMappingDefinition convertBackColumn(final ColumnMetadata columnMetadata)
			throws IOException, ClassNotFoundException {
		final String serialized = columnMetadata.getAdapterNotes();
		return (ColumnMappingDefinition) StringSerializer.deserializeFromString(serialized);
	}

	private SchemaMappingDefinitionToSchemaMetadataConverter(){

    }
}
