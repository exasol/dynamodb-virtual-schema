package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

/**
 * Tests for {@link DynamodbAdapterProperties}.
 */
public class DynamodbAdapterPropertiesTest {

	@Test
	public void testEmptySchema() {
		final AdapterProperties adapterProperties = new AdapterProperties(Collections.emptyMap());
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(false));
	}

	@Test
	public void testHasSchemaDefinitionProperty() {
		final String value = "/bfsdefault/default/mappings/mapping.json";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAPPING", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(true));
	}

	@Test
	public void testGetSchemaDefinitionProperty() throws AdapterException {
		final String value = "/bfsdefault/default/mappings/mapping.json";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAPPING", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.getMappingDefinition(),
				equalTo("/bfsdefault/default/mappings/mapping.json"));
	}
}
