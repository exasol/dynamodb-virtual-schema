package com.exasol.adapter.dynamodb.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.utils.StringSerializer;

/**
 * This class converts a {@link SchemaMappingDefinition} into a
 * {@link SchemaMetadata}. The {@link AbstractColumnMappingDefinition}s are
 * serialized into {@link ColumnMetadata#getAdapterNotes()}. Using
 * {@link #convertBackColumn(ColumnMetadata)} it can get deserialized again.
 */
public class SchemaMappingDefinitionToSchemaMetadataConverter {
	private SchemaMappingDefinitionToSchemaMetadataConverter() {

	}

	/**
	 * Creates a {@link SchemaMetadata} for a given {@link SchemaMappingDefinition}
	 *
	 * @param schemaMappingDefinition
	 *            the {@link SchemaMappingDefinition} to be converted
	 * @return {@link SchemaMetadata}
	 * @throws IOException
	 *             if {@link AbstractColumnMappingDefinition} could not get
	 *             serialized
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
		for (final AbstractColumnMappingDefinition column : tableMappingDefinition.getColumns()) {
			columnDefinitions.add(convertColumn(column));
		}
		final String adapterNotes = "";// Due to a bug in exasol core adapter notes are not stored for tables
		return new TableMetadata(tableMappingDefinition.getDestName(), adapterNotes, columnDefinitions, "");
	}

	private static ColumnMetadata convertColumn(final AbstractColumnMappingDefinition columnMappingDefinition)
			throws IOException {
		final String serialized = StringSerializer.serializeToString(columnMappingDefinition);
		return ColumnMetadata.builder()//
				.name(columnMappingDefinition.getDestinationName())//
				.type(columnMappingDefinition.getDestinationDataType())//
				.defaultValue(columnMappingDefinition.getDestinationDefaultValueLiteral())//
				.nullable(columnMappingDefinition.isDestinationNullable())//
				.adapterNotes(serialized).build();
	}

	/**
	 * Build a {@link AbstractColumnMappingDefinition} from ColumnMetadata.
	 *
	 * @param columnMetadata
	 * @return ColumnMappingDefinition
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static AbstractColumnMappingDefinition convertBackColumn(final ColumnMetadata columnMetadata)
			throws IOException, ClassNotFoundException {
		final String serialized = columnMetadata.getAdapterNotes();
		return (AbstractColumnMappingDefinition) StringSerializer.deserializeFromString(serialized);
	}
}
