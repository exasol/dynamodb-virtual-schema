package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.MappingProvider;

/**
 * Tests for {@link JsonMappingProvider}
 */
public class JsonMappingProviderTest {

	private SchemaMappingDefinition getMappingDefinitionForFileName(final String name)
			throws IOException, JsonMappingProvider.SchemaMappingException {
		final ClassLoader classLoader = JsonMappingProvider.class.getClassLoader();
		final MappingProvider mappingProvider = new JsonMappingProvider(
				new File(classLoader.getResource(name).getFile()));
		return mappingProvider.getSchemaMapping();
	}

	/**
	 * Tests schema load from basicMapping.json
	 * 
	 * @throws IOException
	 */
	@Test
	void testBasicMapping() throws IOException, JsonMappingProvider.SchemaMappingException {
		final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("basicMapping.json");
		final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
		final TableMappingDefinition table = tables.get(0);
		final List<ColumnMappingDefinition> columns = table.getColumns();
		final List<String> columnNames = columns.stream().map(ColumnMappingDefinition::getDestinationName)
				.collect(Collectors.toList());
		final StringColumnMappingDefinition isbnColumn = (StringColumnMappingDefinition) columns.stream()
				.filter(column -> column.getDestinationName().equals("isbn")).findAny().get();
		final StringColumnMappingDefinition nameColumn = (StringColumnMappingDefinition) columns.stream()
				.filter(column -> column.getDestinationName().equals("name")).findAny().get();
		assertAll(() -> assertThat(tables.size(), equalTo(1)), //
				() -> assertThat(table.getDestName(), equalTo("BOOKS")),
				() -> assertThat(columnNames, containsInAnyOrder("isbn", "name", "authorName")),
				() -> assertThat(isbnColumn.getDestinationStringSize(), equalTo(20)),
				() -> assertThat(isbnColumn.getOverflowBehaviour(),
						equalTo(StringColumnMappingDefinition.OverflowBehaviour.EXCEPTION)),
				() -> assertThat(isbnColumn.getLookupFailBehaviour(),
						equalTo(ColumnMappingDefinition.LookupFailBehaviour.EXCEPTION)),
				() -> assertThat(nameColumn.getLookupFailBehaviour(),
						equalTo(ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE)),
				() -> assertThat(nameColumn.getDestinationStringSize(), equalTo(100)),
				() -> assertThat(nameColumn.getOverflowBehaviour(),
						equalTo(StringColumnMappingDefinition.OverflowBehaviour.TRUNCATE)));
	}

	@Test
	void testToJsonMapping() throws IOException, JsonMappingProvider.SchemaMappingException {
		final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("toJsonMapping.json");
		final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
		final TableMappingDefinition table = tables.get(0);
		final List<ColumnMappingDefinition> columns = table.getColumns();
		final List<String> columnNames = columns.stream().map(ColumnMappingDefinition::getDestinationName)
				.collect(Collectors.toList());
		assertAll(() -> assertThat(tables.size(), equalTo(1)), //
				() -> assertThat(table.getDestName(), equalTo("BOOKS")),
				() -> assertThat(columnNames, containsInAnyOrder("isbn", "name", "topics")));
	}

	void testDirectoryRead() {
		// todo
	}
}
