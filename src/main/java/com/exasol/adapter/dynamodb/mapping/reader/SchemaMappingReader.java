package com.exasol.adapter.dynamodb.mapping.reader;

import com.exasol.adapter.dynamodb.mapping.SchemaMapping;

/**
 * Interface for {@link SchemaMapping} readers.
 */
public interface SchemaMappingReader {
    /**
     * Get a {@link SchemaMapping}
     * 
     * @return {@link SchemaMapping}
     */
    public SchemaMapping getSchemaMapping();
}
