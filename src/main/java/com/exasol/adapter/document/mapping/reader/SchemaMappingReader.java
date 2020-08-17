package com.exasol.adapter.document.mapping.reader;

import com.exasol.adapter.document.mapping.SchemaMapping;

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
