package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * A {@link MappingDefinitionFactory} giving a hard coded mapping, with on single column mapping the whole document to a
 * JSON string.
 */
public class HardCodedMappingFactory implements MappingDefinitionFactory {
    @Override
    public SchemaMappingDefinition getSchemaMapping() {
        final TableMappingDefinition table = TableMappingDefinition.rootTableBuilder("testTable", "srcTable")
                .withColumnMappingDefinition(new ToJsonColumnMappingDefinition(
                        new AbstractColumnMappingDefinition.ConstructorParameters("json",
                                new DocumentPathExpression.Builder().build(), LookupFailBehaviour.DEFAULT_VALUE)))
                .build();
        return new SchemaMappingDefinition(List.of(table));
    }
}
