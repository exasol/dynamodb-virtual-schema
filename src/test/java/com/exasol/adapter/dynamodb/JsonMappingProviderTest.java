package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.mapping_definition.SchemaMappingDefinition;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import org.junit.jupiter.api.Test;

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
		final List<TableMetadata> tables = schemaMapping.getDestinationSchema().getTables();
		assertThat(tables.size(), equalTo(1));
		final TableMetadata table = tables.get(0);
		final String tableName = table.getName();
		assertThat(tableName, equalTo("BOOKS"));
		final List<ColumnMetadata> columns = table.getColumns();
		final List<String> columnNames = columns.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
		assertThat(columnNames, containsInAnyOrder("isbn", "name", "authorName"));
	}

	@Test
	void testToJsonMapping() throws IOException, JsonMappingProvider.SchemaMappingException {
		final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("toJsonMapping.json");
		final List<TableMetadata> tables = schemaMapping.getDestinationSchema().getTables();
		assertThat(tables.size(), equalTo(1));
		final TableMetadata table = tables.get(0);
		final String tableName = table.getName();
		assertThat(tableName, equalTo("BOOKS"));
		final List<ColumnMetadata> columns = table.getColumns();
		final List<String> columnNames = columns.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
		assertThat(columnNames, containsInAnyOrder("isbn", "name", "topics"));
	}

/*	@Test
	void testSingleColumnToJsonMapping() throws IOException, JsonMappingProvider.SchemaMappingException {
		final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("singleColumnToTableMapping.json");
	}*/

	void testDirectoryRead() {
		// todo
	}
}
