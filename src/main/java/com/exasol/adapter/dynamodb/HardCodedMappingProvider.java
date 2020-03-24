package com.exasol.adapter.dynamodb;

import java.util.List;

import com.exasol.adapter.dynamodb.mapping_definition.SchemaMappingDefinition;
import com.exasol.adapter.dynamodb.mapping_definition.TableMappingDefinition;
import com.exasol.adapter.dynamodb.mapping_definition.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping_definition.result_walker.IdentityDynamodbResultWalker;

/**
 * A {@link MappingProvider} giving a hard coded mapping, with on single column
 * mapping the whole document to a json string.
 */
public class HardCodedMappingProvider implements MappingProvider {
	@Override
	public SchemaMappingDefinition getSchemaMapping() {
		final TableMappingDefinition table = TableMappingDefinition.builder("testTable", true)
				.withColumnMappingDefinition(
						new ToJsonColumnMappingDefinition("json", new IdentityDynamodbResultWalker()))
				.build();
		return new SchemaMappingDefinition(List.of(table));
	}
}
