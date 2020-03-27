package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

/**
 * Definition of a whole schema (consisting of table definitions) mapping from
 * DynamoDB to Exasol Virtual Schema. An instance of this call represents a
 * whole schema.
 */
public class SchemaMappingDefinition {
	private final List<TableMappingDefinition> tableMappings;

	/**
	 * Constructor.
	 * 
	 * @param tableMappings
	 *            List of {@link TableMappingDefinition}s
	 */
	public SchemaMappingDefinition(final List<TableMappingDefinition> tableMappings) {
		this.tableMappings = tableMappings;
	}

	/**
	 * Gets the {@link TableMappingDefinition}s
	 * 
	 * @return List of {@link TableMappingDefinition}s
	 */
	public List<TableMappingDefinition> getTableMappings() {
		return this.tableMappings;
	}
}
