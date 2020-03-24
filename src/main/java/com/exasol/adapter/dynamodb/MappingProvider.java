package com.exasol.adapter.dynamodb;

import com.exasol.adapter.dynamodb.mapping_definition.SchemaMappingDefinition;

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
