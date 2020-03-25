package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;

/**
 * A {@link MappingFactory} giving a hard coded mapping, with on single column
 * mapping the whole document to a json string.
 */
public class HardCodedMappingFactory implements MappingFactory {
	@Override
	public SchemaMappingDefinition getSchemaMapping() {
		final TableMappingDefinition table = TableMappingDefinition.builder("testTable", true)
				.withColumnMappingDefinition(new ToJsonColumnMappingDefinition("json",
						new IdentityDynamodbResultWalker(), ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE))
				.build();
		return new SchemaMappingDefinition(List.of(table));
	}
}
