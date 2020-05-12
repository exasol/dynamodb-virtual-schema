package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * A {@link SchemaMappingReader} giving a hard coded mapping, with on single column mapping the whole document to a JSON
 * string.
 */
public class HardCodedSchemaMappingReader implements SchemaMappingReader {
    @Override
    public SchemaMapping getSchemaMapping() {
        final TableMapping table = TableMapping.rootTableBuilder("testTable", "srcTable")
                .withColumnMappingDefinition(new ToJsonPropertyToColumnMapping(
                        new AbstractPropertyToColumnMapping.ConstructorParameters("json",
                                new DocumentPathExpression.Builder().build(), LookupFailBehaviour.DEFAULT_VALUE)))
                .build();
        return new SchemaMapping(List.of(table));
    }
}
