package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JsonMappingFactory}
 */
public class JsonMappingFactoryTest {

	private SchemaMappingDefinition getMappingDefinitionForFileName(final String name)
			throws IOException, JsonMappingFactory.SchemaMappingException {
		final ClassLoader classLoader = JsonMappingFactory.class.getClassLoader();
		final MappingFactory mappingFactory = new JsonMappingFactory(new File(classLoader.getResource(name).getFile()));
		return mappingFactory.getSchemaMapping();
	}

	/**
	 * Tests schema load from basicMapping.json
	 * 
	 * @throws IOException
	 */
	@Test
	void testBasicMapping() throws IOException, JsonMappingFactory.SchemaMappingException {
		final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("basicMapping.json");
		final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
		final TableMappingDefinition table = tables.get(0);
		final List<ColumnMappingDefinition> columns = table.getColumns();
		final List<String> columnNames = columns.stream().map(ColumnMappingDefinition::getDestinationName)
				.collect(Collectors.toList());
		final ToStringColumnMappingDefinition isbnColumn = (ToStringColumnMappingDefinition) columns.stream()
				.filter(column -> column.getDestinationName().equals("isbn")).findAny().get();
		final ToStringColumnMappingDefinition nameColumn = (ToStringColumnMappingDefinition) columns.stream()
				.filter(column -> column.getDestinationName().equals("name")).findAny().get();
		assertAll(() -> assertThat(tables.size(), equalTo(1)), //
				() -> assertThat(table.getDestName(), equalTo("BOOKS")),
				() -> assertThat(columnNames, containsInAnyOrder("isbn", "name", "authorName")),
				() -> assertThat(isbnColumn.getDestinationStringSize(), equalTo(20)),
				() -> assertThat(isbnColumn.getOverflowBehaviour(),
						equalTo(ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION)),
				() -> assertThat(isbnColumn.getLookupFailBehaviour(),
						equalTo(ColumnMappingDefinition.LookupFailBehaviour.EXCEPTION)),
				() -> assertThat(nameColumn.getLookupFailBehaviour(),
						equalTo(ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE)),
				() -> assertThat(nameColumn.getDestinationStringSize(), equalTo(100)),
				() -> assertThat(nameColumn.getOverflowBehaviour(),
						equalTo(ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE)));
	}

	@Test
	void testToJsonMapping() throws IOException, JsonMappingFactory.SchemaMappingException {
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

	@Test
	void testException() {
		final String fileName = "invalidToStringMappingAtRootLevel.json";
		final JsonMappingFactory.SchemaMappingException exception = assertThrows(
				JsonMappingFactory.SchemaMappingException.class, () -> getMappingDefinitionForFileName(fileName));
		assertAll(() -> assertThat(exception.getCausingMappingDefinitionFileName(), equalTo(fileName)),
				() -> assertThat(exception.getMessage(),
						equalTo("Error in schema mapping invalidToStringMappingAtRootLevel.json:")),
				() -> assertThat(exception.getCause().getMessage(),
						equalTo("ToString mapping is not allowed at root level")));
	}
}
