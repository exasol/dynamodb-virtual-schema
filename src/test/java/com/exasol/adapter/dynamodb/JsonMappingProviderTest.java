package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JsonMappingProvider}
 */
public class JsonMappingProviderTest {

	private SchemaMappingDefinition getMappingDefinitionForFileName(final String name)
			throws IOException {
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
	void testBasic() throws IOException{
		final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("basicMapping.json");
		assertThat(schemaMapping.getDestinationSchema().getTables().size(), equalTo(1));
		final String tableName = schemaMapping.getDestinationSchema().getTables().get(0).getName();
		assertThat(tableName, equalTo("BOOKS"));
	}

	void testDirectoryRead() {
		// todo
	}
}
