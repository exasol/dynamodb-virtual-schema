package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;

public class SchemaMappingDefinitionToSchemaMetadataConverterTest {
	public SchemaMappingDefinition getSchemaMapping() {
		final TableMappingDefinition table = TableMappingDefinition.builder("testTable", true)
				.withColumnMappingDefinition(new ToJsonColumnMappingDefinition("json",
						new IdentityDynamodbResultWalker(), ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE))
				.build();
		return new SchemaMappingDefinition(List.of(table));
	}

	@Test
	void testConvert() throws IOException {
		final SchemaMappingDefinition schemaMapping = getSchemaMapping();
		final SchemaMetadata schemaMetadata = SchemaMappingDefinitionToSchemaMetadataConverter.convert(schemaMapping);
		final List<TableMetadata> tables = schemaMetadata.getTables();
		assertThat(tables.size(), equalTo(1));
		final TableMetadata firstTable = tables.get(0);
		assertThat(firstTable.getName(), equalTo("testTable"));
		final List<String> columnNames = firstTable.getColumns().stream().map(ColumnMetadata::getName)
				.collect(Collectors.toList());
		assertThat(columnNames, containsInAnyOrder("json"));
	}

	@Test
	void testSerialization() throws IOException, ClassNotFoundException {
		final SchemaMappingDefinition schemaMapping = getSchemaMapping();
		final SchemaMetadata schemaMetadata = SchemaMappingDefinitionToSchemaMetadataConverter.convert(schemaMapping);
		final ColumnMetadata firstColumnMetadata = schemaMetadata.getTables().get(0).getColumns().get(0);
		final ColumnMappingDefinition columnMappingDefinition = SchemaMappingDefinitionToSchemaMetadataConverter
				.convertBackColumn(firstColumnMetadata);
		assertThat(columnMappingDefinition.getDestinationName(), equalTo("json"));
	}

}
