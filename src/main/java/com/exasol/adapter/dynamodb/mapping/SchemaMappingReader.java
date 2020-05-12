package com.exasol.adapter.dynamodb.mapping;

/**
 * Interface for {@link SchemaMapping} readers.
 */
public interface SchemaMappingReader {
    /**
     * Gives a {@link SchemaMapping}
     * 
     * @return {@link SchemaMapping}
     */
    public SchemaMapping getSchemaMapping();
}
