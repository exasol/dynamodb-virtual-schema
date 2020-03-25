package com.exasol.adapter.dynamodb.mapping;

/**
 * Interface for {@link SchemaMappingDefinition} providers.
 */
public interface MappingFactory {
	/**
	 * Gives a {@link SchemaMappingDefinition}
	 * 
	 * @return {@link SchemaMappingDefinition}
	 */
	SchemaMappingDefinition getSchemaMapping();
}
