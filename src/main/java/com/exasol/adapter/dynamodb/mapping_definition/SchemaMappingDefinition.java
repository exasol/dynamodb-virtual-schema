package com.exasol.adapter.dynamodb.mapping_definition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * Definition of a whole schema (consisting of table definitions) mapping from
 * DynamoDB to Exasol Virtual Schema. An instance of this call represents a
 * whole schema.
 */
public class SchemaMappingDefinition {
	private final List<TableMappingDefinition> tableMappings;

	public SchemaMappingDefinition(final List<TableMappingDefinition> tableMappings) {
		this.tableMappings = tableMappings;
	}

	/**
	 * Creates the {@link SchemaMetadata} for this schema.
	 * 
	 * @return {@link SchemaMetadata} for this schema
	 * @throws IOException
	 */
	public SchemaMetadata getDestinationSchema() throws IOException {
		final List<TableMetadata> tableMetadata = new ArrayList<>();
		for (final TableMappingDefinition table : this.tableMappings) {
			final TableMetadata destinationTable = table.getDestinationTable();
			tableMetadata.add(destinationTable);
		}
		return new SchemaMetadata("", tableMetadata);
	}
}
