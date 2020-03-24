package com.exasol.adapter.dynamodb;

import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinition;

/**
 * Interface for {@link SchemaMappingDefinition} providers.
 */
// TODO rename to factory
// TODO move in mapping package
public interface MappingProvider {
	/**
	 * Gives a {@link SchemaMappingDefinition}
	 * 
	 * @return {@link SchemaMappingDefinition}
	 */
	SchemaMappingDefinition getSchemaMapping();
}
