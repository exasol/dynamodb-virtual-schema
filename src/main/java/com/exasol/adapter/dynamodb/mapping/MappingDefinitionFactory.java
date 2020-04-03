package com.exasol.adapter.dynamodb.mapping;

/**
 * Interface for {@link SchemaMappingDefinition} factories.
 */
public interface MappingFactory {
	/**
	 * Gives a {@link SchemaMappingDefinition}
	 * 
	 * @return {@link SchemaMappingDefinition}
	 */
	public SchemaMappingDefinition getSchemaMapping();
}
