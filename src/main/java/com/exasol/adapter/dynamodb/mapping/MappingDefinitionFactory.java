package com.exasol.adapter.dynamodb.mapping;

/**
 * Interface for {@link SchemaMappingDefinition} factories.
 */
public interface MappingDefinitionFactory {
    /**
     * Gives a {@link SchemaMappingDefinition}
     * 
     * @return {@link SchemaMappingDefinition}
     */
    public SchemaMappingDefinition getSchemaMapping();
}
