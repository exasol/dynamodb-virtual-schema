package com.exasol.adapter.dynamodb;

/**
 * Interface for {@link SchemaMappingDefinition} providers.
 */
public interface MappingProvider {
	/**
	 * Gives a {@link SchemaMappingDefinition}
	 * 
	 * @return {@link SchemaMappingDefinition}
	 */
	SchemaMappingDefinition getSchemaMapping();
}
