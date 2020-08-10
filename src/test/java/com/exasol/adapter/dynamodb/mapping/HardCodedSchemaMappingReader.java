package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.reader.SchemaMappingReader;

/**
 * A {@link SchemaMappingReader} giving a hard coded mapping, with on single column mapping the whole document to a JSON
 * string.
 */
public class HardCodedSchemaMappingReader implements SchemaMappingReader {
    @Override
    public SchemaMapping getSchemaMapping() {
        final TableMapping table = TableMapping.rootTableBuilder("testTable", "srcTable")
                .withColumnMappingDefinition(new ToJsonPropertyToColumnMapping("json", DocumentPathExpression.empty(),
                        LookupFailBehaviour.DEFAULT_VALUE, 0,
                        ToJsonPropertyToColumnMapping.OverflowBehaviour.EXCEPTION))
                .build();
        return new SchemaMapping(List.of(table));
    }
}
