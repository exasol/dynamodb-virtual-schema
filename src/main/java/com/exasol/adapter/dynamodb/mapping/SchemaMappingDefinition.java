package com.exasol.adapter.dynamodb.mapping;

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

	public List<TableMappingDefinition> getTableMappings() {
		return this.tableMappings;
	}
}
